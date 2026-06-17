import os
import json
import uuid
import logging
from typing import Optional, Any, Dict

import asyncpg
from datetime import datetime, date
from config import settings

_pool: Optional[asyncpg.pool.Pool] = None


async def init_db_pool(dsn: Optional[str] = None):
    """Initialize a global asyncpg pool from DSN or DATABASE_URL env var.

    Returns the pool or None if no DSN is configured.
    """
    global _pool
    if _pool:
        return _pool
    dsn = dsn or settings.database_url
    logger = logging.getLogger(__name__)
    if not dsn:
        logger.debug("DATABASE_URL not set; DB persistence disabled")
        return None
    _pool = await asyncpg.create_pool(dsn)
    logger.debug("Initialized DB pool")
    return _pool


async def _ensure_pool():
    return _pool if _pool else await init_db_pool()


async def record_step_event(correlation_id: Optional[str], pipeline_name: str, step: Dict[str, Any]):
    logger = logging.getLogger(__name__)
    pool = await _ensure_pool()
    if not pool:
        logger.debug("DB pool not available; skipping record_step_event")
        return

    payload = json.dumps(step, default=str)
    id_str = str(uuid.uuid4())

    try:
        async with pool.acquire() as conn:
            def _parse_dt(val):
                if val is None:
                    return None
                if isinstance(val, (datetime, date)):
                    return val
                if isinstance(val, str):
                    try:
                        # parse ISO 8601 timestamps like '2026-06-17T17:19:38.366663+00:00'
                        return datetime.fromisoformat(val)
                    except Exception:
                        # attempt a forgiving fallback for Z suffix
                        try:
                            if val.endswith("Z"):
                                return datetime.fromisoformat(val.replace("Z", "+00:00"))
                        except Exception:
                            return None
                return None

            started_at = _parse_dt(step.get("started_at"))
            finished_at = _parse_dt(step.get("finished_at"))

            await conn.execute(
                """
                INSERT INTO pipeline_events(
                    id, correlation_id, pipeline_name, step_name, event_type,
                    payload, status, started_at, finished_at, duration_ms, created_at
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
                """,
                id_str,
                correlation_id,
                pipeline_name,
                step.get("name"),
                "step",
                payload,
                step.get("status"),
                started_at,
                finished_at,
                float(step.get("duration_ms") or 0),
            )
    except Exception as ex:
        logger.exception("Failed to persist step event: %s", ex)


async def record_pipeline_event(correlation_id: Optional[str], pipeline_name: str, pipeline_result: Dict[str, Any]):
    logger = logging.getLogger(__name__)
    pool = await _ensure_pool()
    if not pool:
        logger.debug("DB pool not available; skipping record_pipeline_event")
        return

    payload = json.dumps(pipeline_result, default=str)
    id_str = str(uuid.uuid4())

    try:
        async with pool.acquire() as conn:
            def _parse_dt(val):
                if val is None:
                    return None
                if isinstance(val, (datetime, date)):
                    return val
                if isinstance(val, str):
                    try:
                        return datetime.fromisoformat(val)
                    except Exception:
                        try:
                            if val.endswith("Z"):
                                return datetime.fromisoformat(val.replace("Z", "+00:00"))
                        except Exception:
                            return None
                return None

            started_at = _parse_dt(pipeline_result.get("started_at"))
            finished_at = _parse_dt(pipeline_result.get("finished_at"))

            await conn.execute(
                """
                INSERT INTO pipelines(
                    id, correlation_id, pipeline_name, payload, status,
                    started_at, finished_at, duration_ms, errors, created_at
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
                """,
                id_str,
                correlation_id,
                pipeline_name,
                payload,
                pipeline_result.get("status"),
                started_at,
                finished_at,
                float(pipeline_result.get("duration_ms") or 0),
                json.dumps(pipeline_result.get("errors") or []),
            )
    except Exception as ex:
        logger.exception("Failed to persist pipeline event: %s", ex)
