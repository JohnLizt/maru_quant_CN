"""
因子流水线完整性校验。
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from app.factors.base import BaseFactor


def _iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


@dataclass(frozen=True)
class ValidationIssue:
    trade_date: str
    factor_name: str
    expected_count: int
    actual_count: int


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    issues: list[ValidationIssue]


def _expected_counts(engine, asset_type: str, start: str, end: str, symbols: list[str], suspended_policy: str) -> dict[str, int]:
    sql = """
        SELECT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date,
               COUNT(DISTINCT symbol) AS symbol_count
        FROM market.daily
        WHERE asset_type = :asset_type
          AND time >= :start AND time <= :end
          AND symbol = ANY(:symbols)
    """
    if suspended_policy == "mask":
        sql += " AND NOT is_suspended"
    sql += " GROUP BY trade_date"

    with engine.connect() as conn:
        rows = conn.execute(
            text(sql),
            {
                "asset_type": asset_type,
                "start": _iso(start),
                "end": _iso(end),
                "symbols": symbols,
            },
        ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def _actual_counts(engine, asset_type: str, start: str, end: str, symbols: list[str], factor_names: list[str]) -> dict[tuple[str, str], int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date,
                       factor_name,
                       COUNT(DISTINCT symbol) AS symbol_count
                FROM factors.daily_factors
                WHERE asset_type = :asset_type
                  AND time >= :start AND time <= :end
                  AND symbol = ANY(:symbols)
                  AND factor_name = ANY(:factor_names)
                GROUP BY trade_date, factor_name
                """
            ),
            {
                "asset_type": asset_type,
                "start": _iso(start),
                "end": _iso(end),
                "symbols": symbols,
                "factor_names": factor_names,
            },
        ).fetchall()
    return {(r[0], r[1]): int(r[2]) for r in rows}


def get_missing_factor_symbols(
    engine,
    asset_type: str,
    trade_date: str,
    symbols: list[str],
    factor_name: str,
    *,
    limit: int = 5,
) -> list[str]:
    if not symbols:
        return []

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH universe AS (
                    SELECT unnest(:symbols) AS symbol
                )
                SELECT universe.symbol
                FROM universe
                LEFT JOIN factors.daily_factors df
                  ON df.asset_type = :asset_type
                 AND df.symbol = universe.symbol
                 AND df.factor_name = :factor_name
                 AND TO_CHAR(df.time AT TIME ZONE 'UTC', 'YYYYMMDD') = :trade_date
                WHERE df.symbol IS NULL
                ORDER BY universe.symbol
                LIMIT :limit
                """
            ),
            {
                "asset_type": asset_type,
                "trade_date": trade_date,
                "symbols": symbols,
                "factor_name": factor_name,
                "limit": limit,
            },
        ).fetchall()
    return [str(row[0]) for row in rows]


def validate_factor_completeness(
    engine,
    asset_type: str,
    target_dates: list[str],
    symbols: list[str],
    factors: list[BaseFactor],
) -> ValidationResult:
    """校验指定日期和因子集合的覆盖是否完整。"""
    if not target_dates or not factors or not symbols:
        return ValidationResult(ok=True, issues=[])

    start, end = target_dates[0], target_dates[-1]
    actual_counts = _actual_counts(engine, asset_type, start, end, symbols, [factor.name for factor in factors])

    expected_by_policy = {
        policy: _expected_counts(engine, asset_type, start, end, symbols, policy)
        for policy in {factor.suspended_policy for factor in factors}
    }

    issues: list[ValidationIssue] = []
    for trade_date in target_dates:
        for factor in factors:
            expected = expected_by_policy[factor.suspended_policy].get(trade_date, 0)
            actual = actual_counts.get((trade_date, factor.name), 0)
            if actual != expected:
                issues.append(
                    ValidationIssue(
                        trade_date=trade_date,
                        factor_name=factor.name,
                        expected_count=expected,
                        actual_count=actual,
                    )
                )

    return ValidationResult(ok=not issues, issues=issues)


def get_complete_factor_dates(engine, asset_type: str, target_dates: list[str], symbols: list[str], factors: list[BaseFactor]) -> set[str]:
    """返回在指定因子集合下已完整覆盖的交易日。"""
    result = validate_factor_completeness(engine, asset_type, target_dates, symbols, factors)
    incomplete_dates = {issue.trade_date for issue in result.issues}
    return {trade_date for trade_date in target_dates if trade_date not in incomplete_dates}
