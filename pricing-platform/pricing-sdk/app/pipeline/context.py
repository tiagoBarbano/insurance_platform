from dataclasses import dataclass, field


@dataclass
class PipelineContext:
    correlation_id: str
    product: str
    data: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    result: dict = field(default_factory=dict)