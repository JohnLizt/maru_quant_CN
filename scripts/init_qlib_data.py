"""
初始化 Qlib 数据。

用法：
  python scripts/init_qlib_data.py
  python scripts/init_qlib_data.py --market us
"""
from __future__ import annotations

import argparse
import os


def main(market: str = "cn") -> None:
    qlib_data_dir = os.environ.get("QLIB_DATA_DIR", "/app/qlib_data")

    # 延迟导入，避免在不需要时触发依赖加载
    import qlib
    from qlib.config import REG_CN, REG_US
    from qlib.data import D
    from qlib.tests.data import GetData

    if os.path.isdir(os.path.join(qlib_data_dir, "calendars")) and os.path.isdir(
        os.path.join(qlib_data_dir, "instruments")
    ):
        print(f"✅ Qlib 数据已存在于 {qlib_data_dir}，跳过下载")
    else:
        print(f"正在下载 Qlib {market} 数据到 {qlib_data_dir} ...")
        print("（首次下载约 2~5 GB，请耐心等待）")
        GetData().qlib_data(
            target_dir=qlib_data_dir,
            region=market,
            interval="1d",
            exists_skip=True,
        )
        print("✅ 数据下载完成")

    print("正在验证数据...")
    region = REG_CN if market == "cn" else REG_US
    qlib.init(provider_uri=qlib_data_dir, region=region)

    instruments = D.instruments("all")
    cal = D.calendar(freq="day")
    sample = D.list_instruments(instruments, freq="day", as_list=True)

    print(f"交易日历: {cal[0]} ~ {cal[-1]}，共 {len(cal)} 个交易日")
    print(f"股票数量: {len(sample)}")
    print(f"示例代码: {sample[:5]}")
    print()
    print("✅ Qlib 数据初始化完成")
    print(f"数据目录: {qlib_data_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Qlib data")
    parser.add_argument("--market", default="cn", choices=["cn", "us"], help="Qlib market")
    args = parser.parse_args()
    main(args.market)
