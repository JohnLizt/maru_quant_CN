#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# 初始化 Qlib 数据（在 app 容器内执行）
# 使用方法：docker compose exec app bash scripts/init_qlib_data.sh
# 首次运行需下载约 2~5 GB 数据，时间取决于网络速度
# ─────────────────────────────────────────────────────────────
set -e

QLIB_DATA_DIR="${QLIB_DATA_DIR:-/app/qlib_data}"
MARKET="${1:-cn}"   # cn | us

# 若数据目录已存在且非空，跳过下载
if [ -d "${QLIB_DATA_DIR}/calendars" ] && [ -d "${QLIB_DATA_DIR}/instruments" ]; then
    echo "✅ Qlib 数据已存在于 ${QLIB_DATA_DIR}，跳过下载"
else
    echo "正在下载 Qlib ${MARKET} 数据到 ${QLIB_DATA_DIR} ..."
    echo "（首次下载约 2~5 GB，请耐心等待）"

    python -c "
from qlib.tests.data import GetData
GetData().qlib_data(
    target_dir='${QLIB_DATA_DIR}',
    region='${MARKET}',
    interval='1d',
    exists_skip=True,
)
"

    echo "✅ 数据下载完成"
fi

# 验证数据可用性
echo "正在验证数据..."
python - <<EOF
import qlib
from qlib.data import D
from qlib.config import REG_CN, REG_US

region = REG_CN if "${MARKET}" == "cn" else REG_US
qlib.init(provider_uri="${QLIB_DATA_DIR}", region=region)

instruments = D.instruments('all')
cal = D.calendar(freq='day')
print(f"交易日历: {cal[0]} ~ {cal[-1]}，共 {len(cal)} 个交易日")

sample = D.list_instruments(instruments, freq='day', as_list=True)
print(f"股票数量: {len(sample)}")
print(f"示例代码: {sample[:5]}")
EOF

echo ""
echo "✅ Qlib 数据初始化完成"
echo "数据目录: ${QLIB_DATA_DIR}"
