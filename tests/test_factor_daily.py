from scripts.factor_daily import _warmup_start


def test_warmup_start_uses_calendar_buffer_for_trading_day_windows() -> None:
    assert _warmup_start("20250707", 30) == "20250428"
