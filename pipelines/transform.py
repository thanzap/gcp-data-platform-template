"""Validate and normalise raw API rows into the warehouse schema.

Bad rows are collected, not silently dropped: the caller decides (via
`max_reject_ratio`) whether a handful of malformed rows is acceptable or a
sign that the upstream format changed and the load must stop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

SCHEMA = [  # mirrors schemas/revenue_daily.json
    ("date", "DATE"),
    ("source", "STRING"),
    ("site_domain", "STRING"),
    ("ad_unit", "STRING"),
    ("country", "STRING"),
    ("impressions", "INT64"),
    ("revenue_eur", "NUMERIC"),
    ("loaded_at", "TIMESTAMP"),
]
KEY = ["date", "source", "site_domain", "ad_unit", "country"]


class TooManyRejects(ValueError):
    pass


@dataclass
class Result:
    rows: list[dict] = field(default_factory=list)
    rejects: list[tuple[dict, str]] = field(default_factory=list)

    @property
    def reject_ratio(self) -> float:
        total = len(self.rows) + len(self.rejects)
        return len(self.rejects) / total if total else 0.0


def _domain(value) -> str:
    s = str(value or "").strip().lower()
    for prefix in ("https://", "http://", "www."):
        s = s.removeprefix(prefix)
    return s.split("/")[0]


def normalise(
    raw_rows: list[dict],
    day: date,
    source: str,
    fx_to_eur: dict[str, Decimal],
    max_reject_ratio: float = 0.01,
    loaded_at: datetime | None = None,
) -> Result:
    loaded_at = loaded_at or datetime.now(UTC)
    result = Result()
    for raw in raw_rows:
        try:
            currency = str(raw.get("currency") or "EUR").upper()
            if currency not in fx_to_eur:
                raise ValueError(f"no FX rate for {currency}")
            impressions = int(raw["impressions"])
            revenue = Decimal(str(raw["revenue"]))
            if impressions < 0 or revenue < 0:
                raise ValueError("negative metric")
            domain = _domain(raw.get("site_domain"))
            if not domain:
                raise ValueError("missing site_domain")
            result.rows.append({
                "date": day.isoformat(),
                "source": source,
                "site_domain": domain,
                "ad_unit": str(raw.get("ad_unit") or "(none)"),
                "country": str(raw.get("country") or "ZZ").upper()[:2],
                "impressions": impressions,
                "revenue_eur": str((revenue * fx_to_eur[currency]).quantize(Decimal("0.000001"), ROUND_HALF_UP)),
                "loaded_at": loaded_at.isoformat(),
            })
        except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
            result.rejects.append((raw, str(exc)))

    if result.reject_ratio > max_reject_ratio:
        sample = "; ".join(reason for _, reason in result.rejects[:5])
        raise TooManyRejects(f"{len(result.rejects)} of {len(raw_rows)} rows rejected (e.g. {sample})")
    return result
