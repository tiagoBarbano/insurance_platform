"""Exemplo de pipeline e steps demonstrando o uso do runtime `pipeline`.

Este módulo contém pequenos passos de demonstração e mostra como instanciar
e usar o `HttpIntegrator` para chamadas externas com JWT/OAuth.

Execute com:
    uv run app/pipeline/run_pipeline.py

Arquitetura resumo:
- `StepA`, `StepB`, `DiscountStep`: passos simples que alteram `context.result`.
- `DemoHttpStep`: usa `HttpIntegrator` para fazer um GET e grava o resultado.
- `OAuthDemoStep`: demonstra uso de token pré-existente ou integração OAuth2.
"""

import asyncio
import uuid

from pipeline import Pipeline, BaseStep, ParallelStep, StepResult
from context import PipelineContext
from enums import StepStatus

from http_integrator import HttpIntegrator


class StepA(BaseStep):
    """Exemplo de step simples.

    Uso: escreve `a=1` em `context.result` e retorna `StepResult` com payload.
    Ideal para testar o fluxo sequencial do `Pipeline`.
    """

    async def run(self, context: PipelineContext) -> StepResult:
        context.result["a"] = 1
        return StepResult(status=StepStatus.SUCCESS, output={"a": 1})


class StepB(BaseStep):
    """Outro passo de exemplo que grava `b=2` em `context.result`.

    Use como parte de um `ParallelStep` para demonstrar execução concorrente.
    """

    async def run(self, context: PipelineContext) -> StepResult:
        context.result["b"] = 2
        return StepResult(status=StepStatus.SUCCESS, output={"b": 2})


class DiscountStep(BaseStep):
    """Exemplo de step que calcula/aplica desconto.

    Comportamento:
      - Se `context.data.has_discount` for falso, retorna `SKIPPED`.
      - Caso contrário, aplica um desconto fictício e adiciona a `context.result`.

    Em um SDK real a lógica seria extraída para um serviço ou injetada via
    dependência para facilitar testes.
    """

    async def run(self, context: PipelineContext) -> StepResult:
        if not context.data.get("has_discount"):
            return StepResult(status=StepStatus.SKIPPED, message="No discount")

        # exemplo de execução; em um SDK real, a lógica seria injetada ou sobrescrita
        try:
            # executar lógica de desconto (placeholder)
            discount_value = 10
            context.result.setdefault("discounts", []).append(discount_value)
            return StepResult(
                status=StepStatus.SUCCESS, output={"discount": discount_value}
            )
        except Exception as ex:
            return StepResult(status=StepStatus.FAILED, message=str(ex))


class DemoHttpStep(BaseStep):
    def __init__(self, integrator: HttpIntegrator, url: str | None = None):
        super().__init__(name="DemoHttpStep")
        self.integrator = integrator
        self.url = url

    async def run(self, context: PipelineContext) -> StepResult:
        url = self.url or context.data.get("demo_url", "https://httpbin.org/get")
        try:
            resp = await self.integrator.get(url)
            context.result.setdefault("http", {}).update(resp if isinstance(resp, dict) else {"body": resp})
            return StepResult(status=StepStatus.SUCCESS, output=resp)
        except Exception as ex:
            return StepResult(status=StepStatus.FAILED, message=str(ex))


class OAuthDemoStep(BaseStep):
    """Exemplo de Step que demonstra uso de OAuth2 (client credentials) ou token pré-existente.

    Comportamento:
    - Se for passada configuração de client credentials (token_url, client_id, client_secret)
      no construtor, o integrador tenta buscar o token automaticamente.
    - Se o contexto contiver 'oauth_token', usa esse token via set_bearer_token.
    - Caso contrário, retorna SKIPPED.
    """

    def __init__(self, integrator: HttpIntegrator, url: str | None = None):
        super().__init__(name="OAuthDemoStep")
        self.integrator = integrator
        self.url = url

    async def run(self, context: PipelineContext) -> StepResult:
        target = self.url or context.data.get("oauth_demo_url", "https://httpbin.org/bearer")

        # prefer explicit token in context
        token = context.data.get("oauth_token")
        if token:
            self.integrator.set_bearer_token(token, expires_in=context.data.get("oauth_expires_in"))

        # otherwise, rely on integrator having OAuth2 config already
        elif not getattr(self.integrator, "has_oauth2_config", lambda: False)():
            return StepResult(status=StepStatus.SKIPPED, message="No OAuth token or integrator OAuth2 config provided")

        try:
            resp = await self.integrator.get(target)
            context.result.setdefault("oauth", {}).update(resp if isinstance(resp, dict) else {"body": resp})
            return StepResult(status=StepStatus.SUCCESS, output=resp)
        except Exception as ex:
            return StepResult(status=StepStatus.FAILED, message=str(ex))


async def main() -> None:
    correlation_id = f"cid-{uuid.uuid4()}"
    ctx = PipelineContext(correlation_id=correlation_id, product="test", data={"has_discount": False})

    # integrator configurado para gerar JWT localmente
    integrator_jwt = HttpIntegrator()
    integrator_jwt.configure_jwt_hs256(secret="my-very-secret", issuer="pricing-sdk", subject="demo", expiry_seconds=60)

    # integrator configurado para OAuth2 (client credentials)
    integrator_oauth = HttpIntegrator()
    # exemplo de configuração; substitua pelos valores reais quando for testar
    integrator_oauth.configure_oauth2(
        token_url="https://auth.example.com/oauth2/token",
        client_id="example-client-id",
        client_secret="example-client-secret",
        scope="api.read",
    )

    pipeline = Pipeline(
        StepA(),
        ParallelStep(StepB(), DiscountStep(), name="ParallelExample"),
        # usa integrator JWT
        DemoHttpStep(integrator_jwt),
        # usa integrator OAuth
        OAuthDemoStep(integrator_oauth),
    )

    result = await pipeline.execute(ctx)
    # close integrator sessions
    await integrator_jwt.close()
    await integrator_oauth.close()

    print("Output:", result.output)
    print("Errors:", result.errors)

if __name__ == "__main__":
    asyncio.run(main(), debug=False)
