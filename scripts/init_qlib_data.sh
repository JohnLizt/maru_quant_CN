#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# 初始化 Qlib 数据（在 app 容器内执行）
# 使用方法：docker compose exec app bash scripts/init_qlib_data.sh
# ─────────────────────────────────────────────────────────────
set -e

QLIB_DATA_DIR="${QLIB_DATA_DIR:-/app/qlib_data}"
MARKET="${1:-cn}"   # cn | us

echo "正在下载 Qlib ${MARKET} 数据到 ${QLIB_DATA_DIR} ..."

python - <<EOF
import qlib
from qlib.data import D
from qlib.config import REG_CN

# 初始化 Qlib（中国 A 股）
qlib.init(provider_uri="$QLIB_DATA_DIR", region=REG_CN)
print("Qlib 初始化成功")
print("可用字段:", D.features(D.instruments('all'), ['$close', '$volume'], freq='day'))
EOF

echo "✅ Qlib 数据初始化完成"
echo "数据目录: ${QLIB_DATA_DIR}"
