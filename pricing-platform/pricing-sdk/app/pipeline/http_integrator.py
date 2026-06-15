import aiohttp
import time
import hmac
import hashlib
import base64
import json
from typing import Optional, Any, Dict, Mapping


class HttpIntegrator:
    """Async HTTP integrator built on `aiohttp` with optional auth helpers.

    Features:
    - Minimal async request helpers: `get`, `post`, and a generic `request`.
    - Automatic Authorization header injection using, in order of priority:
        1) explicit bearer token set with `set_bearer_token()`
        2) OAuth2 client credentials flow via `configure_oauth2()` (auto-fetch)
        3) locally-generated JWT (HS256) via `configure_jwt_hs256()`

    Usage examples:
        integrator = HttpIntegrator(timeout=5)
        # simple GET
        resp = await integrator.get("https://httpbin.org/get")

        # use a pre-issued bearer token
        integrator.set_bearer_token("token", expires_in=300)
        resp = await integrator.get("https://api.example.com/data")

        # configure OAuth2 (client credentials). The integrator will fetch
        # an access token automatically before making requests when needed.
        integrator.configure_oauth2(token_url, client_id, client_secret, scope="api.read")
        resp = await integrator.get("https://api.example.com/protected")

        # configure local JWT HS256 generation (no external token server)
        integrator.configure_jwt_hs256(secret="s3cr3t", issuer="my-app", subject="demo")
        resp = await integrator.get("https://api.example.com/protected")

    Notes:
    - `close()` should be called when the integrator is no longer needed to
        properly close the aiohttp session (or use `async with HttpIntegrator()`).
    - For production use, avoid logging secrets and consider secure storage
        for client secrets.
    """

    def __init__(self, timeout: int = 10, headers: Optional[Dict[str, str]] = None):
        self._session: Optional[aiohttp.ClientSession] = None
        self.timeout = timeout
        self.default_headers = headers or {}
        # token cache and configs
        self._access_token: Optional[str] = None
        self._token_expires_at: float | None = None
        self._oauth2_config: Optional[Dict[str, str]] = None
        self._jwt_config: Optional[Dict[str, Any]] = None

    async def _ensure_session(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.default_headers)

    async def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Perform an HTTP request and return a dict with `status`, `body`,
        and `headers`.

        The method will attempt to attach an `Authorization` header based on
        configured token / oauth2 / jwt settings.
        """
        await self._ensure_session()
        to = timeout or self.timeout
        hdrs = {} if headers is None else headers

        try:
            # attach auth header if configured
            auth_hdr = await self._get_auth_header()
            if auth_hdr:
                hdrs = {**hdrs, **auth_hdr}

            client_timeout = aiohttp.ClientTimeout(total=to)
            async with self._session.request(
                method.upper(),
                url,
                params=params,
                json=json,
                headers=hdrs,
                timeout=client_timeout,
            ) as resp:
                status = resp.status
                content_type = resp.headers.get("Content-Type", "")
                body: Any
                if "application/json" in content_type:
                    try:
                        body = await resp.json()
                    except Exception:
                        body = await resp.text()
                else:
                    body = await resp.text()

                return {"status": status, "body": body, "headers": dict(resp.headers)}

        except Exception:
            raise

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self.request(
            "GET", url, params=params, headers=headers, timeout=timeout
        )

    async def post(
        self,
        url: str,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self.request(
            "POST", url, json=json, headers=headers, timeout=timeout
        )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    # --- Auth helpers ---
    def configure_oauth2(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None,
    ) -> None:
        """Configure OAuth2 client credentials flow for automatic token fetching.

        The integrator will POST the client credentials to `token_url` using
        `grant_type=client_credentials` and cache the `access_token` for the
        duration of `expires_in` returned by the token endpoint.
        """
        self._oauth2_config = {
            "token_url": token_url,
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope or "",
        }

    def has_oauth2_config(self) -> bool:
        """Return True if OAuth2 client-credentials config is present."""
        return bool(self._oauth2_config)

    def has_jwt_config(self) -> bool:
        """Return True if JWT HS256 config is present."""
        return bool(self._jwt_config)

    def configure_jwt_hs256(
        self,
        secret: str,
        issuer: Optional[str] = None,
        subject: Optional[str] = None,
        expiry_seconds: int = 300,
        additional_claims: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Configure local JWT HS256 generation parameters.

        When configured, the integrator will generate a short-lived JWT using
        HMAC-SHA256 on each request (unless an explicit bearer token is set).
        Useful for services that accept locally-signed tokens.
        """
        self._jwt_config = {
            "secret": secret,
            "issuer": issuer,
            "subject": subject,
            "expiry_seconds": int(expiry_seconds),
            "additional_claims": dict(additional_claims or {}),
        }

    def set_bearer_token(self, token: str, expires_in: Optional[int] = None) -> None:
        """Set an explicit bearer token to be used for subsequent requests.

        `expires_in` is the lifetime in seconds from now; when expired the
        integrator will fall back to oauth/jwt configuration if present.
        """
        self._access_token = token
        self._token_expires_at = (time.time() + expires_in) if expires_in else None

    async def _get_auth_header(self) -> Dict[str, str] | None:
        # priority: explicit token > oauth2 config > jwt config
        if self._access_token and (
            self._token_expires_at is None or time.time() < self._token_expires_at
        ):
            return {"Authorization": f"Bearer {self._access_token}"}

        if self._oauth2_config:
            # attempt to fetch and cache an OAuth2 token; failures are
            # intentionally non-fatal and simply leave authorization unset.
            await self._fetch_oauth2_token()
            if self._access_token:
                return {"Authorization": f"Bearer {self._access_token}"}

        if self._jwt_config:
            token = self._generate_jwt_hs256()
            return {"Authorization": f"Bearer {token}"}

        return None

    async def _fetch_oauth2_token(self) -> None:
        cfg = self._oauth2_config
        if not cfg:
            return

        token_url = cfg["token_url"]
        data = {
            "grant_type": "client_credentials",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
        }
        if cfg.get("scope"):
            data["scope"] = cfg.get("scope")

        # Note: this may call external endpoint; use short timeout
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    token_url, data=data, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        access_token = body.get("access_token")
                        expires_in = body.get("expires_in")
                        if access_token:
                            self._access_token = access_token
                            self._token_expires_at = (
                                (time.time() + int(expires_in)) if expires_in else None
                            )
        except Exception:
            # leave token unset on failure; caller will handle missing auth
            return

    def _generate_jwt_hs256(self) -> str:
        cfg = self._jwt_config or {}
        secret = cfg.get("secret", "")
        header = {"alg": "HS256", "typ": "JWT"}
        iat = int(time.time())
        exp = iat + int(cfg.get("expiry_seconds", 300))
        payload = {"iat": iat, "exp": exp}
        if cfg.get("issuer"):
            payload["iss"] = cfg["issuer"]
        if cfg.get("subject"):
            payload["sub"] = cfg["subject"]
        payload.update(cfg.get("additional_claims", {}))

        def _b64url(data: bytes) -> str:
            return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

        header_b = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        payload_b = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        signing_input = f"{header_b}.{payload_b}".encode("ascii")
        signature = hmac.new(
            secret.encode("utf-8"), signing_input, hashlib.sha256
        ).digest()
        signature_b = _b64url(signature)
        token = f"{header_b}.{payload_b}.{signature_b}"
        return token
