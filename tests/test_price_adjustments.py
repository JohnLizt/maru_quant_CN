from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.data_loader.market_data import update_daily_adjustments
from app.factors.pipeline.loader import load_ohlcv
from app.factors.pipeline.writer import upsert_factors
from app.utils.price_adjustment import apply_price_adjustment
from scripts.backfill_yahoo_adj_factors import _validate_and_select


def test_apply_price_adjustment_changes_only_prices_and_drops_factor() -> None:
    frame = pl.DataFrame(
        {
            "open": [100.0, 200.0, 300.0],
            "high": [110.0, 210.0, 310.0],
            "low": [90.0, 190.0, 290.0],
            "close": [105.0, 205.0, 305.0],
            "ohlc4": [101.25, 201.25, 301.25],
            "volume": [10, 20, 30],
            "amount": [1000.0, 4000.0, 9000.0],
            "adj_factor": [0.5, None, -1.0],
        }
    )

    adjusted = apply_price_adjustment(frame)

    assert "adj_factor" not in adjusted.columns
    assert adjusted.get_column("close").to_list() == pytest.approx([52.5, 205.0, 305.0])
    assert adjusted.get_column("ohlc4").to_list() == pytest.approx([50.625, 201.25, 301.25])
    assert adjusted.get_column("volume").to_list() == [10, 20, 30]
    assert adjusted.get_column("amount").to_list() == pytest.approx([1000.0, 4000.0, 9000.0])


def test_factor_loader_applies_adjustment_but_preserves_amount() -> None:
    captured: dict[str, str] = {}

    class _Result:
        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                (
                    datetime(2026, 1, 2, tzinfo=timezone.utc),
                    "etf_US",
                    "QQQ",
                    100.0,
                    1000.0,
                    False,
                    0.8,
                )
            ]

    class _Connection:
        def execute(self, statement: object, _params: dict[str, object]) -> _Result:
            captured["sql"] = str(statement)
            return _Result()

        def __enter__(self) -> _Connection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class _Engine:
        def connect(self) -> _Connection:
            return _Connection()

    loaded = load_ohlcv(
        _Engine(),
        "etf_US",
        "QQQ",
        "20260101",
        "20260103",
        {"close", "amount"},
    )

    assert "adj_factor" in captured["sql"]
    assert loaded.columns == ["time", "asset_type", "symbol", "close", "amount", "is_suspended"]
    assert loaded.get_column("close").to_list() == pytest.approx([80.0])
    assert loaded.get_column("amount").to_list() == pytest.approx([1000.0])


def test_update_daily_adjustments_updates_only_adjustment_columns() -> None:
    captured: dict[str, object] = {"sql": []}

    class _Result:
        rowcount = 1

    class _Connection:
        connection: _DriverConnection

        def __init__(self) -> None:
            self.connection = _DriverConnection()

        def execute(self, statement: object) -> _Result:
            captured["sql"].append(str(statement))  # type: ignore[union-attr]
            return _Result()

    class _Cursor:
        def copy_expert(self, statement: str, buffer: object) -> None:
            captured["copy_sql"] = statement
            captured["copy_data"] = buffer.read()  # type: ignore[attr-defined]

        def __enter__(self) -> _Cursor:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class _DriverConnection:
        driver_connection: _DriverConnection

        def __init__(self) -> None:
            self.driver_connection = self

        def cursor(self) -> _Cursor:
            return _Cursor()

    class _Transaction:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *_args: object) -> None:
            return None

    class _Engine:
        def begin(self) -> _Transaction:
            return _Transaction()

    adjustments = pl.DataFrame(
        {
            "time": [date(2026, 1, 2)],
            "asset_type": ["etf_US"],
            "symbol": ["QQQ"],
            "adj_factor": [0.75],
            "pct_change": [1.25],
        }
    )

    assert update_daily_adjustments(adjustments, engine=_Engine()) == 1
    sql = "\n".join(captured["sql"])  # type: ignore[arg-type]
    assert "SET adj_factor" in sql
    assert "pct_change" in sql
    assert "open" not in sql
    assert "close" not in sql
    assert "QQQ" in str(captured["copy_data"])


def test_backfill_validation_rejects_missing_database_date() -> None:
    daily = pl.DataFrame(
        {
            "time": [datetime(2026, 1, 2, tzinfo=timezone.utc)],
            "asset_type": ["etf_US"],
            "symbol": ["QQQ"],
            "adj_factor": [0.9],
            "pct_change": [None],
        }
    )

    with pytest.raises(ValueError, match="缺少 1 个数据库交易日"):
        _validate_and_select(
            daily,
            asset_type="etf_US",
            symbol="QQQ",
            expected_dates={date(2026, 1, 2), date(2026, 1, 5)},
        )


def test_factor_writer_uses_bulk_copy_and_upsert() -> None:
    captured: dict[str, object] = {"sql": []}

    class _Result:
        rowcount = 1

    class _Cursor:
        def copy_expert(self, statement: str, buffer: object) -> None:
            captured["copy_sql"] = statement
            captured["copy_data"] = buffer.read()  # type: ignore[attr-defined]

        def __enter__(self) -> _Cursor:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class _DriverConnection:
        driver_connection: _DriverConnection

        def __init__(self) -> None:
            self.driver_connection = self

        def cursor(self) -> _Cursor:
            return _Cursor()

    class _Connection:
        connection: _DriverConnection

        def __init__(self) -> None:
            self.connection = _DriverConnection()

        def execute(self, statement: object) -> _Result:
            captured["sql"].append(str(statement))  # type: ignore[union-attr]
            return _Result()

    class _Transaction:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *_args: object) -> None:
            return None

    class _Engine:
        def begin(self) -> _Transaction:
            return _Transaction()

    factors = pl.DataFrame(
        {
            "time": [datetime(2026, 1, 2, tzinfo=timezone.utc)],
            "asset_type": ["etf_US"],
            "symbol": ["QQQ"],
            "factor_name": ["momentum_reg_20"],
            "factor_value": [1.5],
        }
    )

    assert upsert_factors(_Engine(), factors) == 1
    sql = "\n".join(captured["sql"])  # type: ignore[arg-type]
    assert "ON CONFLICT" in sql
    assert "COPY _daily_factors_upsert" in str(captured["copy_sql"])
    assert "momentum_reg_20" in str(captured["copy_data"])
