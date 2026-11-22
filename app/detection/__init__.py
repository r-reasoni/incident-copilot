"""Online window feature extraction and anomaly detection."""
from __future__ import annotations

from app.detection.detector import AnomalyDetector, WindowResult
from app.detection.windowing import ClosedWindow, WindowAggregator

__all__ = [
    "AnomalyDetector",
    "ClosedWindow",
    "WindowAggregator",
    "WindowResult",
]
