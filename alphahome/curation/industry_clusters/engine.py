"""Pure, deterministic multi-view minimax clustering; no database writes."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import heapq
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ClusterConfig:
    version: str = "industry_minimax_v2"
    windows: tuple[int, int] = (120, 252)
    minimum_days: tuple[int, int] = (100, 200)
    industry_weight: float = 0.45
    holdings_weight: float = 0.20
    behavior_weight: float = 0.35
    industry_l2_weight: float = 0.75
    entry_radius: float = 0.25
    retention_radius: float = 0.30
    pair_l1_floor: float = 0.60
    pair_l2_floor: float = 0.45
    entry_l1: float = 0.75
    entry_l2: float = 0.65
    retention_l1: float = 0.70
    retention_l2: float = 0.55
    entry_correlation: float = 0.90
    retention_correlation: float = 0.85
    entry_residual_correlation: float = 0.80
    retention_residual_correlation: float = 0.70
    entry_volatility_ratio: float = 1.30
    retention_volatility_ratio: float = 1.50
    entry_tracking_error: float = 0.15
    retention_tracking_error: float = 0.20
    confirmation_months: int = 2
    review_months: tuple[int, ...] = (3, 6, 9, 12)
    major_constituent_overlap: float = 0.50
    representative_improvement: float = 0.02
    maximum_structure_age_days: int = 183
    maximum_classification_age_days: int = 183
    maximum_price_staleness_sessions: int = 2
    benchmark: str = "000985.CSI"
    allow_partial_classification: bool = True
    isolate_unready_members: bool = True
    selection_data_policy: str = "stock_fallback"

    def __post_init__(self):
        if not np.isclose(self.industry_weight + self.holdings_weight + self.behavior_weight, 1):
            raise ValueError("feature block weights must sum to one")
        if min(self.industry_weight, self.holdings_weight, self.behavior_weight) < 0:
            raise ValueError("negative feature weight")
        if len(self.windows) != len(self.minimum_days) or not self.windows:
            raise ValueError("window/minimum pairs required")
        if any(n < 3 or n > w for w, n in zip(self.windows, self.minimum_days)):
            raise ValueError("invalid minimum observations")
        if self.entry_radius > self.retention_radius or self.confirmation_months < 1:
            raise ValueError("invalid maintenance configuration")
        for key in ("l1", "l2", "correlation", "residual_correlation"):
            if getattr(self, "entry_" + key) < getattr(self, "retention_" + key):
                raise ValueError("entry thresholds must be stricter than retention")
        if self.entry_volatility_ratio > self.retention_volatility_ratio or self.entry_tracking_error > self.retention_tracking_error:
            raise ValueError("entry risk thresholds must be stricter than retention")
        if len(set(self.windows)) != len(self.windows) or any(w < 3 for w in self.windows):
            raise ValueError("windows must be unique positive lengths")
        for key in ("industry_l2_weight", "entry_radius", "retention_radius", "pair_l1_floor", "pair_l2_floor",
                    "entry_l1", "entry_l2", "retention_l1", "retention_l2", "major_constituent_overlap"):
            if not 0 <= getattr(self, key) <= 1:
                raise ValueError(f"{key} must be within [0,1]")
        for key in ("entry_correlation", "retention_correlation", "entry_residual_correlation", "retention_residual_correlation"):
            if not -1 <= getattr(self, key) <= 1:
                raise ValueError(f"{key} must be within [-1,1]")
        if self.entry_volatility_ratio < 1 or self.entry_tracking_error < 0:
            raise ValueError("invalid risk distance limits")
        if min(self.maximum_structure_age_days, self.maximum_classification_age_days) < 1 or any(m not in range(1, 13) for m in self.review_months):
            raise ValueError("invalid age or review calendar")
        if self.maximum_price_staleness_sessions < 0:
            raise ValueError("price staleness cannot be negative")
        if self.selection_data_policy not in ("strict", "stock_fallback"):
            raise ValueError("unknown selection data policy")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ClusterConfig":
        data = dict(data)
        if data.get("version") == "industry_minimax_v1":
            data.setdefault("allow_partial_classification", False)
            data.setdefault("isolate_unready_members", False)
            data.setdefault("selection_data_policy", "strict")
        for key in ("windows", "minimum_days", "review_months"):
            if key in data:
                data[key] = tuple(data[key])
        return cls(**data)


@dataclass
class FeatureSet:
    asof: str
    codes: tuple[str, ...]
    distance: np.ndarray
    stock_overlap: np.ndarray
    l1_overlap: np.ndarray
    l2_overlap: np.ndarray
    correlations: dict[int, np.ndarray]
    residual_correlations: dict[int, np.ndarray]
    volatility_ratios: dict[int, np.ndarray]
    tracking_errors: dict[int, np.ndarray]
    counts: dict[int, np.ndarray]
    coverage: pd.DataFrame
    weights: dict[str, dict[str, float]]
    config: ClusterConfig
    position: dict[str, int] = field(init=False)

    def __post_init__(self):
        self.position = {code: i for i, code in enumerate(self.codes)}

    def ready(self, code: str) -> bool:
        i = self.position[code]
        return all(np.isfinite(self.correlations[w][i, i]) and np.isfinite(self.residual_correlations[w][i, i]) for w in self.config.windows)

    def multiview_ready(self, code: str) -> bool:
        i = self.position[code]
        return bool(self.ready(code) and self.l1_overlap[i, i] >= self.config.entry_l1 - 1e-12
                    and self.l2_overlap[i, i] >= self.config.entry_l2 - 1e-12)

    def subset(self, codes) -> "FeatureSet":
        codes = tuple(sorted(codes))
        p = [self.position[c] for c in codes]
        take = lambda x: x[np.ix_(p, p)]
        return FeatureSet(self.asof, codes, take(self.distance), take(self.stock_overlap),
                          take(self.l1_overlap), take(self.l2_overlap),
                          *[{w: take(x) for w, x in block.items()} for block in
                            (self.correlations, self.residual_correlations, self.volatility_ratios, self.tracking_errors, self.counts)],
                          self.coverage.loc[self.coverage.index_code.isin(codes)].copy(),
                          {c: self.weights[c] for c in codes}, self.config)

    def semantic_ok(self, members) -> bool:
        p = [self.position[c] for c in members]
        return bool((self.l1_overlap[np.ix_(p, p)] >= self.config.pair_l1_floor - 1e-12).all()
                    and (self.l2_overlap[np.ix_(p, p)] >= self.config.pair_l2_floor - 1e-12).all())

    def evaluate(self, members, mode: str = "entry", preferred: str | None = None) -> dict:
        """Return all feasible representatives; missing observations are unknown."""
        members = tuple(sorted(members))
        if not members or mode not in ("entry", "retention"):
            raise ValueError("nonempty members and valid mode required")
        p = [self.position[c] for c in members]
        sem = self.semantic_ok(members)
        known = all(self.ready(c) for c in members)
        block = self.distance[np.ix_(p, p)]
        valid = (self.l1_overlap[np.ix_(p, p)] >= getattr(self.config, mode + "_l1") - 1e-12)
        valid &= self.l2_overlap[np.ix_(p, p)] >= getattr(self.config, mode + "_l2") - 1e-12
        for w in self.config.windows:
            raw = self.correlations[w][np.ix_(p, p)]
            res = self.residual_correlations[w][np.ix_(p, p)]
            known &= bool(np.isfinite(raw).all() and np.isfinite(res).all())
            valid &= raw >= getattr(self.config, mode + "_correlation") - 1e-12
            valid &= res >= getattr(self.config, mode + "_residual_correlation") - 1e-12
            valid &= self.volatility_ratios[w][np.ix_(p, p)] <= getattr(self.config, mode + "_volatility_ratio") + 1e-12
            valid &= self.tracking_errors[w][np.ix_(p, p)] <= getattr(self.config, mode + "_tracking_error") + 1e-12
        radii = np.max(block, axis=1)
        valid_reps = np.flatnonzero(valid.all(axis=1) & (radii <= getattr(self.config, mode + "_radius") + 1e-12)) if sem else []
        ordered = sorted(valid_reps, key=lambda k: (round(float(radii[k]), 12), round(float(block[k].mean()), 12), members[k]))
        feasible = [members[k] for k in ordered]
        chosen = preferred if preferred in feasible else (feasible[0] if feasible else None)
        radius = float(radii[members.index(chosen)]) if chosen else None
        return {"valid": bool(feasible), "known": bool(known), "semantic_ok": sem,
                "representative": chosen, "radius": radius, "feasible_representatives": feasible}

    def quality(self, members, representative) -> dict:
        members = tuple(sorted(members))
        p = [self.position[c] for c in members]
        r = self.position[representative] if representative in self.position else p[0]

        def finite_extreme(x, kind="min"):
            return float(np.min(x) if kind == "min" else np.max(x)) if np.isfinite(x).all() else None

        row: dict[str, Any] = {
            "worst_member_stock_overlap": float(self.stock_overlap[r, p].min()),
            "worst_member_l1_overlap": float(self.l1_overlap[r, p].min()),
            "worst_member_l2_overlap": float(self.l2_overlap[r, p].min()),
            "worst_pair_l2_overlap": float(self.l2_overlap[np.ix_(p, p)].min()),
            "radius": finite_extreme(self.distance[r, p], "max"),
        }
        for w in self.config.windows:
            row.update({
                f"worst_member_corr_{w}": finite_extreme(self.correlations[w][r, p]),
                f"worst_member_residual_corr_{w}": finite_extreme(self.residual_correlations[w][r, p]),
                f"worst_pair_corr_{w}": finite_extreme(self.correlations[w][np.ix_(p, p)]),
                f"max_volatility_ratio_{w}": finite_extreme(self.volatility_ratios[w][r, p], "max"),
                f"max_tracking_error_{w}": finite_extreme(self.tracking_errors[w][r, p], "max"),
                f"minimum_common_days_{w}": int(self.counts[w][r, p].min()),
            })
        return row


def _overlap(frame: pd.DataFrame) -> np.ndarray:
    if frame.empty and len(frame) == 0:
        return np.zeros((0, 0))
    values = frame.to_numpy(float)
    return np.clip(np.array([np.minimum(row, values).sum(axis=1) for row in values]), 0, 1)


def quality_checked_classification(classification: pd.DataFrame) -> pd.DataFrame:
    """Unknown or rejected quality contributes no known industry weight."""
    classification = classification.copy()
    if "data_quality" in classification:
        invalid = ~classification.data_quality.isin(("normal", "high"))
        classification.loc[invalid, ["industry_code1", "industry_code2"]] = None
    return classification


def build_features(members: pd.DataFrame, classification: pd.DataFrame,
                   returns: pd.DataFrame, asof, config: ClusterConfig | None = None) -> FeatureSet:
    """Inputs are the latest known full-index, non-proxy structure at the cutoff."""
    config = config or ClusterConfig()
    date = pd.Timestamp(asof).normalize()
    required = {"index_code", "ts_code", "weight"}
    if not required.issubset(members) or members.duplicated(["index_code", "ts_code"]).any():
        raise ValueError("invalid constituent keys")
    if not members.weight.ge(0).all() or not np.isfinite(members.weight).all():
        raise ValueError("invalid constituent weights")
    if not members.ts_code.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all():
        raise ValueError("industry clusters only accept pure A-share constituents")
    if not members.groupby("index_code").weight.sum().sub(1).abs().le(1e-6).all():
        raise ValueError("constituent weights do not sum to one")
    for field_name in ("source_effective_date", "source_available_date", "obs_date"):
        if field_name in members and pd.to_datetime(members[field_name]).gt(date).any():
            raise ValueError("future constituent information")
    if classification.duplicated("ts_code").any():
        raise ValueError("one industry classification per stock required")
    if "obs_date" in classification and pd.to_datetime(classification.obs_date).gt(date).any():
        raise ValueError("future industry classification")
    classification = quality_checked_classification(classification)
    joined = members[list(required)].merge(classification[["ts_code", "industry_code1", "industry_code2"]],
                                          on="ts_code", how="left", validate="many_to_one")
    if not config.allow_partial_classification and joined[["industry_code1", "industry_code2"]].isna().any().any():
        raise ValueError("unclassified stock weight cannot be replaced with zero")
    codes = tuple(sorted(members.index_code.unique()))
    stock = members.pivot(index="index_code", columns="ts_code", values="weight").fillna(0).reindex(codes)
    # Unknown mass stays outside every known industry. Do not normalize it away
    # or pool it into one shared "unknown industry" that would add similarity.
    vectors = [joined.groupby(["index_code", f"industry_code{level}"]).weight.sum().unstack(fill_value=0).reindex(codes).fillna(0)
               for level in (1, 2)]
    s, l1, l2 = _overlap(stock), _overlap(vectors[0]), _overlap(vectors[1])
    if returns.index.duplicated().any() or not returns.index.is_monotonic_increasing:
        raise ValueError("returns require a unique sorted trading calendar")
    raw_matrices, residual_matrices, ratios, errors, counts, coverage = {}, {}, {}, {}, {}, []
    behavior = np.zeros_like(s)
    for window, minimum in zip(config.windows, config.minimum_days):
        sample = returns.loc[:date].tail(window).reindex(columns=list(codes) + [config.benchmark])
        local = sample[list(codes)]
        residuals = pd.DataFrame(np.nan, index=sample.index, columns=codes)
        for code in codes:
            paired = sample[[code, config.benchmark]].dropna()
            beta = None
            valid_positions = np.flatnonzero(local[code].notna().to_numpy())
            staleness = len(local) - 1 - int(valid_positions[-1]) if len(valid_positions) else len(local)
            fresh = staleness <= config.maximum_price_staleness_sessions
            if fresh and len(paired) >= minimum and paired[config.benchmark].std() > 1e-12:
                x = np.column_stack([np.ones(len(paired)), paired[config.benchmark]])
                coef = np.linalg.lstsq(x, paired[code].to_numpy(), rcond=None)[0]
                residuals.loc[paired.index, code] = paired[code] - x @ coef
                beta = float(coef[1])
            coverage.append({"asof_date": date.date().isoformat(), "index_code": code, "window": window,
                             "return_days": int(local[code].notna().sum()), "market_common_days": len(paired),
                             "minimum_days": minimum, "beta": beta,
                             "price_staleness_sessions": staleness, "price_fresh": fresh,
                             "annual_volatility": float(local[code].std() * np.sqrt(252)),
                             "known_l1_weight": float(vectors[0].loc[code].sum()),
                             "known_l2_weight": float(vectors[1].loc[code].sum())})
        raw = local.corr(min_periods=minimum).to_numpy()
        residual = residuals.corr(min_periods=minimum).to_numpy()
        raw_matrices[window], residual_matrices[window] = raw, residual
        valid = local.notna().to_numpy(dtype=np.int32)
        values = local.fillna(0).to_numpy(float)
        n = valid.T @ valid
        sums, squares, cross = values.T @ valid, (values * values).T @ valid, values.T @ values
        with np.errstate(invalid="ignore", divide="ignore"):
            var = np.maximum((squares - sums * sums / n) / (n - 1), 0)
            cov = (cross - sums * sums.T / n) / (n - 1)
            vr = np.sqrt(np.maximum(var, var.T) / np.minimum(var, var.T))
            te = np.sqrt(np.maximum(var + var.T - 2 * cov, 0) * 252)
        vr[n < minimum] = np.nan
        te[n < minimum] = np.nan
        ratios[window], errors[window], counts[window] = vr, te, n
        behavior += ((1 - np.clip(raw, -1, 1)) + (1 - np.clip(residual, -1, 1))) / (2 * len(config.windows))
    industry_distance = (1 - config.industry_l2_weight) * (1 - l1) + config.industry_l2_weight * (1 - l2)
    distance = config.industry_weight * industry_distance + config.holdings_weight * (1 - s) + config.behavior_weight * behavior
    # Unknown similarity is infinite for proposed merging, never zero correlation.
    distance = np.where(np.isfinite(distance), np.maximum(distance, 0), np.inf)
    np.fill_diagonal(distance, 0)
    weights = {code: {str(k): float(v) for k, v in stock.loc[code].items() if v > 0} for code in codes}
    return FeatureSet(date.date().isoformat(), codes, distance, s, l1, l2,
                      raw_matrices, residual_matrices, ratios, errors, counts,
                      pd.DataFrame(coverage, columns=[
                          "asof_date", "index_code", "window", "return_days", "market_common_days", "minimum_days",
                          "beta", "price_staleness_sessions", "price_fresh", "annual_volatility",
                          "known_l1_weight", "known_l2_weight"]), weights, config)


def fit_minimax(features: FeatureSet, starting_groups=None) -> list[tuple[str, ...]]:
    """Agglomerate admissible unions by minimax radius; ties use sorted codes."""
    groups = [tuple(sorted(g)) for g in (starting_groups if starting_groups is not None else [(c,) for c in features.codes])]
    flat = [c for g in groups for c in g]
    if len(flat) != len(set(flat)) or set(flat) != set(features.codes):
        raise ValueError("starting groups must partition the feature universe")
    active = {i: g for i, g in enumerate(sorted(groups))}
    next_id = len(active)
    heap = []

    def consider(a, b):
        union = tuple(sorted(active[a] + active[b]))
        result = features.evaluate(union)
        if result["valid"]:
            heapq.heappush(heap, (round(result["radius"], 12), union, a, b))

    for a in active:
        for b in range(a):
            consider(a, b)
    while heap:
        _, union, a, b = heapq.heappop(heap)
        if a not in active or b not in active:
            continue
        del active[a], active[b]
        active[next_id] = union
        for other in tuple(active):
            if other != next_id:
                consider(other, next_id)
        next_id += 1
    return sorted(active.values())


def fit_stock_overlap(features: FeatureSet, threshold: float = 0.60) -> list[tuple[str, ...]]:
    """Frozen old single-linkage control."""
    parent = list(range(len(features.codes)))

    def root(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(parent)):
        for j in np.flatnonzero(features.stock_overlap[i, :i] >= threshold - 1e-12):
            a, b = root(i), root(int(j))
            parent[max(a, b)] = min(a, b)
    groups = {}
    for i, code in enumerate(features.codes):
        groups.setdefault(root(i), []).append(code)
    return sorted(tuple(v) for v in groups.values())
