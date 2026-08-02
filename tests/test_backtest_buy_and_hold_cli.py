from __future__ import annotations

from datetime import date

from app.cli.backtest_buy_and_hold import _find_first_trading_date, build_parser


def test_buy_and_hold_parser_defaults_to_clean_vti_baseline() -> None:
    args = build_parser().parse_args([])

    assert args.symbol == "VTI"
    assert args.asset_type == "etf_US"
    assert args.start_date == "2016-08-24"
    assert args.execution_lag == 0
    assert args.commission_bps == 1.5
    assert args.slippage_bps == 2.5
    assert args.commission_min == 0.0
    assert args.cash_interest_rate == 0.0
    assert args.save_artifacts is True
    assert args.save_chart is True


def test_find_first_trading_date_uses_first_available_market_row() -> None:
    captured: dict[str, object] = {}

    class _Result:
        def scalar_one_or_none(self) -> date:
            return date(2026, 1, 5)

    class _Connection:
        def execute(self, statement: object, params: dict[str, object]) -> _Result:
            captured["sql"] = str(statement)
            captured["params"] = params
            return _Result()

        def __enter__(self) -> _Connection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class _Engine:
        def connect(self) -> _Connection:
            return _Connection()

    first_date = _find_first_trading_date(
        _Engine(),
        asset_type="etf_US",
        symbol="VTI",
        start_date=date(2026, 1, 3),
        end_date=date(2026, 1, 31),
    )

    assert first_date == date(2026, 1, 5)
    assert captured["params"] == {
        "asset_type": "etf_US",
        "symbol": "VTI",
        "start_date": date(2026, 1, 3),
        "end_date": date(2026, 1, 31),
    }
    assert "MIN(time)::date" in str(captured["sql"])
