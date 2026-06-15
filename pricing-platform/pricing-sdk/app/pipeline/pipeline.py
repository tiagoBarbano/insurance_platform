import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, List, Union

from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import perf_counter

from context import PipelineContext
from enums import StepStatus


@dataclass
class StepResult:
    """Represents the outcome of a single step execution.

    Attributes:
      status: `StepStatus` enum indicating SUCCESS/FAILED/SKIPPED.
      message: Optional human-readable message (usually used for errors or skip reasons).
      output: Optional structured output produced by the step (serializable).

    Usage example:
      return StepResult(status=StepStatus.SUCCESS, output={"value": 123})
      return StepResult(status=StepStatus.FAILED, message="validation failed")
    """
    status: StepStatus
    message: str | None = None
    output: Any | None = None

    def to_dict(self) -> dict:
        return {
            "status": self.status.value
            if isinstance(self.status, StepStatus)
            else str(self.status),
            "message": self.message,
            "output": self.output,
        }


@dataclass
class StepExecution:
    """Immutable record of a single step run used in pipeline reporting.

    This object is produced by the pipeline runtime and contains timestamps
    and measured `duration_ms` for observability.

    Fields mirror the serialized form returned in `PipelineResult.to_dict()`.

    Do not construct this when implementing steps; return a `StepResult` and
    let the `Pipeline` runtime convert it to a `StepExecution`.
    """
    name: str
    status: StepStatus
    started_at: datetime
    finished_at: datetime
    duration_ms: float
    output: Any | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status.value
            if isinstance(self.status, StepStatus)
            else str(self.status),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_ms": self.duration_ms,
            "output": self.output,
            "error": self.error,
        }


@dataclass
class ParallelResult:
    """Result container for a `ParallelStep`.

    Holds a list of `StepExecution` entries for each parallel child step,
    plus aggregated counters (`succeeded`, `failed`, `skipped`).

    A `ParallelStep` returns this object directly from its `execute` method
    so the parent `Pipeline` can include nested step information in the
    overall `PipelineResult`.
    """
    name: str
    status: StepStatus
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    steps: List[StepExecution]
    succeeded: int
    failed: int
    skipped: int

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status.value
            if isinstance(self.status, StepStatus)
            else str(self.status),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_ms": self.duration_ms,
            "steps": [s.to_dict() for s in self.steps],
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
        }


@dataclass
class PipelineResult:
    """Top-level result produced after running a `Pipeline`.

    Contains metadata (correlation id, product), timing (started_at, finished_at,
    duration_ms), overall status and the ordered list of step results which
    may include `StepExecution` or nested `ParallelResult` items.

    Use `to_dict()` to produce a JSON-serializable representation for logs
    or API responses.
    """
    correlation_id: str
    product: str | None
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    status: StepStatus
    steps: List[Union[StepExecution, ParallelResult]]
    output: dict
    errors: List[str]

    def to_dict(self) -> dict:
        def serialize_step(s):
            if hasattr(s, "to_dict"):
                return s.to_dict()
            return s

        return {
            "correlation_id": self.correlation_id,
            "product": self.product,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_ms": self.duration_ms,
            "status": self.status.value
            if isinstance(self.status, StepStatus)
            else str(self.status),
            "steps": [serialize_step(s) for s in self.steps],
            "output": self.output,
            "errors": self.errors,
        }


class Step(ABC):
    """Abstract base class for pipeline steps.

    Implementers must provide `async def execute(self, context)` which runs
    the step's behavior. The pipeline runtime calls `execute()` and expects
    a `StepResult` (or a `ParallelResult` for parallel composites).

    Consider subclassing `BaseStep` to inherit standard timing/logging
    behavior and to implement `run()` instead of `execute()`.
    """
    name: str

    @abstractmethod
    async def execute(self, context: PipelineContext) -> StepResult:
        raise NotImplementedError()


class BaseStep(Step):
    """Convenience base class for most steps.

    Provides automatic debug logging and timing around the `run()` method.
    Subclasses should implement `async def run(self, context: PipelineContext)`
    and return a `StepResult`.

    Example:
      class MyStep(BaseStep):
          async def run(self, context):
              # business logic
              return StepResult(status=StepStatus.SUCCESS, output={})
    """
    def __init__(self, name: str | None = None):
        self.name = name or self.__class__.__name__

    async def execute(self, context: PipelineContext) -> StepResult:
        logger = logging.getLogger(__name__)
        logger.debug("Starting step %s", self.name)
        start = perf_counter()
        try:
            result = await self.run(context)
            return result
        except Exception as ex:
            logger.exception("Step %s failed", self.name)
            return StepResult(status=StepStatus.FAILED, message=str(ex))
        finally:
            elapsed = (perf_counter() - start) * 1000
            logger.debug("Finished step %s (elapsed_ms=%.3f)", self.name, elapsed)

    @abstractmethod
    async def run(self, context: PipelineContext) -> StepResult:
        raise NotImplementedError()


class ParallelStep(Step):
    """Composite step that executes multiple child steps concurrently.

    The constructor accepts child `Step` instances. When executed the
    `ParallelStep` runs all children with `asyncio.gather`, captures per-child
    timings and returns a `ParallelResult` containing `StepExecution` entries
    for each child.

    Example:
        p = ParallelStep(StepA(), StepB(), name="FetchAll")
        result = await p.execute(context)
    """
    def __init__(self, *steps, name: str | None = None):
        self.steps = steps
        self.name = name or "Parallel"

    async def execute(self, context: PipelineContext) -> ParallelResult:
        started_at = datetime.now(timezone.utc)
        start_perf = perf_counter()
        logger = logging.getLogger(__name__)

        async def run_step(step):
            s_started_at = datetime.now(timezone.utc)
            s_start_perf = perf_counter()
            try:
                res = await step.execute(context)
                s_finished_at = datetime.now(timezone.utc)
                s_finished_perf = perf_counter()
                return (step, res, s_started_at, s_finished_at, s_start_perf, s_finished_perf)
            except Exception as ex:
                s_finished_at = datetime.now(timezone.utc)
                s_finished_perf = perf_counter()
                return (step, ex, s_started_at, s_finished_at, s_start_perf, s_finished_perf)

        results = await asyncio.gather(*[run_step(s) for s in self.steps], return_exceptions=False)

        executions: List[StepExecution] = []

        for step, result, s_started_at, s_finished_at, s_start_perf, s_finished_perf in results:
            duration_ms = float((s_finished_perf - s_start_perf) * 1000)
            if isinstance(result, Exception):
                logger.debug("Parallel step %s failed: %s", step.__class__.__name__, result)
                executions.append(
                    StepExecution(
                        name=step.__class__.__name__,
                        status=StepStatus.FAILED,
                        started_at=s_started_at,
                        finished_at=s_finished_at,
                        duration_ms=duration_ms,
                        error=str(result),
                    )
                )
            elif isinstance(result, StepResult):
                executions.append(
                    StepExecution(
                        name=step.__class__.__name__,
                        status=result.status,
                        started_at=s_started_at,
                        finished_at=s_finished_at,
                        duration_ms=duration_ms,
                        output=result.output,
                        error=result.message,
                    )
                )
            else:
                logger.debug("Parallel step %s returned raw result", step.__class__.__name__)
                executions.append(
                    StepExecution(
                        name=step.__class__.__name__,
                        status=StepStatus.SUCCESS,
                        started_at=s_started_at,
                        finished_at=s_finished_at,
                        duration_ms=duration_ms,
                        output=result,
                    )
                )

        finished_perf = perf_counter()
        finished_at = datetime.now(timezone.utc)

        succeeded = sum(1 for e in executions if e.status == StepStatus.SUCCESS)
        failed = sum(1 for e in executions if e.status == StepStatus.FAILED)
        skipped = sum(1 for e in executions if e.status == StepStatus.SKIPPED)

        status = StepStatus.SUCCESS if failed == 0 else StepStatus.FAILED

        return ParallelResult(
            name=self.name,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=float((finished_perf - start_perf) * 1000),
            steps=executions,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
        )


class Pipeline:
    """Orchestrates a sequence of `Step` instances.

    Create with the ordered steps and call `await pipeline.execute(context)` to
    run them. The pipeline stops on fatal step failures (status `FAILED`) and
    returns a `PipelineResult` describing the full run.

    Example:
        pipeline = Pipeline(StepA(), ParallelStep(S1,S2), FinalStep())
        result = await pipeline.execute(ctx)
        print(result.to_dict())
    """
    def __init__(self, *steps):
        self.steps = steps

    async def execute(self, context: PipelineContext) -> PipelineResult:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logger.debug("Starting pipeline execution for correlation_id=%s", getattr(context, "correlation_id", None))

        started_at = datetime.now(timezone.utc)
        start_perf = perf_counter()
        executions: List[Union[StepExecution, ParallelResult]] = []
        errors: List[str] = []

        for step in self.steps:
            try:
                logger.debug("Executing step %s", step.__class__.__name__)
                execution = await self._execute_step(step, context)
                executions.append(execution)

                # stop on fatal failure
                if (
                    isinstance(execution, StepExecution)
                    and execution.status == StepStatus.FAILED
                ):
                    errors.append(execution.error or "")
                    break

            except Exception as ex:
                finished_perf = perf_counter()
                now = datetime.now(timezone.utc)
                executions.append(
                    StepExecution(
                        name=step.__class__.__name__,
                        status=StepStatus.FAILED,
                        started_at=now,
                        finished_at=now,
                        duration_ms=(finished_perf - start_perf) * 1000,
                        error=str(ex),
                    )
                )

                errors.append(str(ex))
                break

        finished_perf = perf_counter()
        finished_at = datetime.now(timezone.utc)

        overall_status = StepStatus.FAILED if errors else StepStatus.SUCCESS
        logger.debug("Pipeline finished (duration_ms=%.3f) status=%s", (finished_perf - start_perf) * 1000, overall_status)

        return PipelineResult(
            correlation_id=context.correlation_id,
            product=getattr(context, "product", None),
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=(finished_perf - start_perf) * 1000,
            status=overall_status,
            steps=executions,
            output=context.result,
            errors=errors,
        )

    async def _execute_step(self, step, context: PipelineContext):
        logger = logging.getLogger(__name__)
        started_at = datetime.now(timezone.utc)
        start_perf = perf_counter()

        logger.debug("_execute_step start %s", step.__class__.__name__)
        output = await step.execute(context)
        finished_perf = perf_counter()
        finished_at = datetime.now(timezone.utc)
        logger.debug("_execute_step finished %s (elapsed_ms=%.3f)", step.__class__.__name__, (finished_perf - start_perf) * 1000)

        # If this is a ParallelResult keep it as-is
        if isinstance(output, ParallelResult):
            return output

        # expect StepResult
        status = output.status if isinstance(output, StepResult) else StepStatus.SUCCESS

        return StepExecution(
            name=step.__class__.__name__,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=(finished_perf - start_perf) * 1000,
            output=(output.output if isinstance(output, StepResult) else output),
            error=(output.message if isinstance(output, StepResult) else None),
        )
