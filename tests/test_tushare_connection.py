"""Test that Tushare Pro can fetch data from the network."""
import os

import tushare as ts


def test_tushare_fetch_daily():
    token = os.environ.get("TUSHARE_TOKEN", "")
    ts.set_token(token)
    pro = ts.pro_api()

    print("\nFetching 000001.SZ daily data from Tushare...")
    df = pro.daily(ts_code="000001.SZ", start_date="20250101", end_date="20250103")
    print(f"Rows returned: {len(df)}")
    print(df)
    assert not df.empty
    assert "close" in df.columns
    print("OK")
