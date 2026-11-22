"""
app/detection/detector.py
=========================
Online anomaly detection on windowed log feature vectors using River HalfSpaceTrees.
Maintains a sliding lookback of normal scores for dynamic thresholding with run tracking.
"""
from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any, Callable

import numpy as np
import river.anomaly
import river.preprocessing
from sqlalchemy import update

from app.config import settings
from app.db.models import WindowScore
from app.db.repository import save_window_score
from app.db.session import SessionLocal
from app.detection.windowing import ClosedWindow

logger = logging.getLogger(__name__)


@dataclass
class WindowResult:
    """Result of scoring one time window for anomalies."""

    window_start: datetime
    window_end: datetime
    score: float
    threshold: float | None
    is_anomalous: bool
    features: dict[str, float]
    run_start: datetime | None = None
    confirmed_at: datetime | None = None


# HST scores are not meaningful until the first HST_WINDOW_SIZE windows have been learned,
# which is why the warm-up exists. During warm-up, the reference tree depths and mass distributions
# are populated with baseline normal traffic patterns.


class AnomalyDetector:
    """Online anomaly detector combining MinMaxScaler and HalfSpaceTrees with dynamic thresholding."""

    def __init__(
        self,
        warmup_windows: int | None = None,
        hst_n_trees: int | None = None,
        hst_height: int | None = None,
        hst_window_size: int | None = None,
        threshold_lookback: int | None = None,
        threshold_std_mult: float | None = None,
        threshold_ceiling: float | None = None,
        suspect_windows: int | None = None,
        consecutive_high_windows: int | None = None,
        seed: int = 42,
        save_to_db: bool = True,
        on_window: Callable[[WindowResult], None] | None = None,
        session_factory: Any | None = None,
    ) -> None:
        self.warmup_windows: int = (
            warmup_windows if warmup_windows is not None else settings.warmup_windows
        )
        self.hst_n_trees: int = (
            hst_n_trees if hst_n_trees is not None else settings.hst_n_trees
        )
        self.hst_height: int = (
            hst_height if hst_height is not None else settings.hst_height
        )
        base_hst_window = (
            hst_window_size if hst_window_size is not None else settings.hst_window_size
        )
        # HST effective window size:
        # River's HalfSpaceTrees requires learning HST_WINDOW_SIZE windows to establish
        # its initial reference tree mass distributions before scores become meaningful.
        # To guarantee that the model finishes its initial reference window during warm-up
        # and starts populating threshold history with valid scores before detection begins:
        # effective_hst_window = min(HST_WINDOW_SIZE, max(5, WARMUP_WINDOWS // 2))
        self.hst_window_size: int = min(base_hst_window, max(5, self.warmup_windows // 2))
        logger.info(
            "Initialized AnomalyDetector: effective_hst_window=%d (configured=%d, warmup_windows=%d)",
            self.hst_window_size,
            base_hst_window,
            self.warmup_windows,
        )

        self.threshold_lookback: int = (
            threshold_lookback
            if threshold_lookback is not None
            else settings.threshold_lookback
        )
        self.threshold_std_mult: float = (
            threshold_std_mult
            if threshold_std_mult is not None
            else settings.threshold_std_mult
        )
        self.threshold_ceiling: float = (
            threshold_ceiling
            if threshold_ceiling is not None
            else settings.threshold_ceiling
        )
        self.suspect_windows: int = (
            suspect_windows
            if suspect_windows is not None
            else settings.suspect_windows
        )
        self.consecutive_high_windows: int = (
            consecutive_high_windows
            if consecutive_high_windows is not None
            else settings.consecutive_high_windows
        )
        self.seed: int = seed
        self.save_to_db: bool = save_to_db
        self.on_window: Callable[[WindowResult], None] | None = on_window
        self.session_factory = session_factory or SessionLocal

        # Initialize River Pipeline: MinMaxScaler | HalfSpaceTrees
        self._scaler = river.preprocessing.MinMaxScaler()
        self._model = river.anomaly.HalfSpaceTrees(
            n_trees=self.hst_n_trees,
            height=self.hst_height,
            window_size=self.hst_window_size,
            seed=self.seed,
        )

        # Pre-seed MinMaxScaler min/max bounds using generic domain rules per feature type.
        # These bounds prevent division by zero or extreme out-of-bounds leaf collapse in HST:
        # 1. Ratios/shares (error_rate, warn_rate): strictly bounded in [0.0, 1.0].
        # 2. Latency metrics (duration_mean_ms, duration_p95_ms): plausible service response times
        #    range from 0ms to 10,000ms (10s max HTTP client timeout). In log1p space: [0, log1p(10000)].
        # 3. Counts & rates (log_rate, new_template_count, distinct_template_count, tpl_other, err_*):
        #    plausible event volume is 0 to 1,000 events/second or counts/window. In log1p space: [0, log1p(1000)].
        default_services = getattr(settings, "services", ["api-service", "worker-service"])
        for feat in ["duration_mean_ms", "duration_p95_ms"]:
            self._scaler.min[feat].update(0.0)
            self._scaler.max[feat].update(math.log1p(10000.0))

        for feat in [
            "log_rate",
            "new_template_count",
            "distinct_template_count",
            "tpl_other",
            *[f"err_{svc}" for svc in default_services],
        ]:
            self._scaler.min[feat].update(0.0)
            self._scaler.max[feat].update(math.log1p(1000.0))

        for feat in ["error_rate", "warn_rate"]:
            self._scaler.min[feat].update(0.0)
            self._scaler.max[feat].update(1.0)

        # State tracking
        self._lock = Lock()
        self.windows_scored: int = 0
        self._score_history: deque[float] = deque(maxlen=self.threshold_lookback)
        self._consecutive_high_count: int = 0
        self._current_run_start: datetime | None = None
        self._quarantine_remaining: int = 0

    def _transform_features(self, features: dict[str, float]) -> dict[str, float]:
        """Apply log1p to count, duration, and rate-derived features."""
        x: dict[str, float] = {}
        for k, v in features.items():
            val = float(v)
            if k in ("error_rate", "warn_rate"):
                x[k] = val
            else:
                x[k] = math.log1p(max(0.0, val))
        return x

    def _compute_threshold(self) -> float | None:
        """Compute threshold = min(max(mean + THRESHOLD_STD_MULT*std, 0.9 quantile), THRESHOLD_CEILING) over lookback."""
        if len(self._score_history) < 2:
            return None
        scores = np.array(self._score_history)
        mean_val = float(np.mean(scores))
        std_val = float(np.std(scores))
        q90 = float(np.quantile(scores, 0.90))
        computed = max(mean_val + self.threshold_std_mult * std_val, q90, 0.01)
        return min(computed, self.threshold_ceiling)

    def score_window(self, window: ClosedWindow | dict[str, Any]) -> WindowResult:
        """Score a closed window, update anomaly tracking, and persist to database."""
        if isinstance(window, ClosedWindow):
            w_start = window.window_start
            w_end = window.window_end
            raw_features = dict(window.features)
        else:
            w_start = window["window_start"]
            w_end = window["window_end"]
            raw_features = dict(window["features"])

        x = self._transform_features(raw_features)

        with self._lock:
            self.windows_scored += 1
            is_warmup = self.windows_scored <= self.warmup_windows

            # 1. Transform and score using River pipeline
            x_scaled = self._scaler.transform_one(x)
            score = float(self._model.score_one(x_scaled))

            # 2. Determine threshold
            threshold = self._compute_threshold()

            is_anomalous = False
            run_start: datetime | None = None
            confirmed_at: datetime | None = None

            if is_warmup:
                # During warm-up: always learn and never flag
                self._scaler.learn_one(x)
                x_scaled_for_learn = self._scaler.transform_one(x)
                self._model.learn_one(x_scaled_for_learn)
                if not getattr(self._model, "_first_window", False):
                    self._score_history.append(score)
                self._consecutive_high_count = 0
                self._current_run_start = None
                self._quarantine_remaining = 0
            else:
                # Post warm-up
                is_high = threshold is not None and score >= threshold

                if is_high:
                    self._consecutive_high_count += 1
                    if self._current_run_start is None:
                        self._current_run_start = w_start

                    if self._consecutive_high_count >= self.consecutive_high_windows:
                        is_anomalous = True
                        run_start = self._current_run_start
                        if self._consecutive_high_count == self.consecutive_high_windows:
                            confirmed_at = w_end

                    # Any window with score >= threshold starts suspect quarantine
                    self._quarantine_remaining = self.suspect_windows
                else:
                    self._consecutive_high_count = 0
                    self._current_run_start = None

                    # If score < threshold, check suspect quarantine
                    if self._quarantine_remaining > 0:
                        # Suspect-window quarantine: NOT learned and NOT added to threshold history
                        self._quarantine_remaining -= 1
                    else:
                        # Normal clean window: learn and add to threshold history
                        self._scaler.learn_one(x)
                        x_scaled_for_learn = self._scaler.transform_one(x)
                        self._model.learn_one(x_scaled_for_learn)
                        self._score_history.append(score)

            result = WindowResult(
                window_start=w_start,
                window_end=w_end,
                score=score,
                threshold=threshold,
                is_anomalous=is_anomalous,
                features=raw_features,
                run_start=run_start,
                confirmed_at=confirmed_at,
            )

            # Persist every result with repository.save_window_score
            if self.save_to_db:
                self._persist_result(result)

            if self.on_window is not None:
                try:
                    self.on_window(result)
                except Exception:
                    logger.exception("Error in detector on_window callback")

            return result

    def process_window(self, window: ClosedWindow | dict[str, Any]) -> WindowResult:
        """Alias for score_window()."""
        return self.score_window(window)

    def _persist_result(self, result: WindowResult) -> None:
        try:
            db_features = dict(result.features)
            if result.threshold is not None:
                db_features["_threshold"] = result.threshold
            if result.run_start is not None:
                db_features["_run_start"] = result.run_start.isoformat()
            if result.confirmed_at is not None:
                db_features["_confirmed_at"] = result.confirmed_at.isoformat()

            with self.session_factory() as session, session.begin():
                save_window_score(
                    session,
                    window_start=result.window_start,
                    window_end=result.window_end,
                    score=result.score,
                    is_anomalous=result.is_anomalous,
                    features=db_features,
                )
                if (
                    result.is_anomalous
                    and result.run_start is not None
                    and result.run_start < result.window_start
                ):
                    session.execute(
                        update(WindowScore)
                        .where(
                            WindowScore.window_start >= result.run_start,
                            WindowScore.window_start <= result.window_start,
                        )
                        .values(is_anomalous=True)
                    )
        except Exception:
            logger.exception("Failed to persist window score to database")
