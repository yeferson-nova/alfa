import json
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Sequence

UNITS = frozenset({"Count", "Milliseconds", "Seconds", "Bytes", "None"})


@dataclass(frozen=True)
class Metric:
    name: str
    value: float
    unit: str = "Count"


def emf_line(
    namespace: str,
    metrics: Sequence[Metric],
    dimensions: Mapping[str, str],
    timestamp: datetime,
) -> str:
    if not namespace.strip():
        raise ValueError("namespace must not be empty")
    if not metrics:
        raise ValueError("at least one metric is required")
    unknown = sorted({metric.unit for metric in metrics} - UNITS)
    if unknown:
        raise ValueError(f"unknown metric units: {unknown}")
    document = {
        "_aws": {
            "Timestamp": int(timestamp.timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [sorted(dimensions)],
                    "Metrics": [{"Name": metric.name, "Unit": metric.unit} for metric in metrics],
                }
            ],
        },
        **{key: str(value) for key, value in dimensions.items()},
        **{metric.name: metric.value for metric in metrics},
    }
    return json.dumps(document, ensure_ascii=False)
