# A股量化系统

> Tushare + TimescaleDB + Polars + Qlib

## 架构概览

```
┌─────────────────────────────────────────────────┐
│                  Docker Network                  │
│                                                  │
│  ┌──────────┐   ┌──────────┐   ┌─────────────┐  │
│  │Tushare   │──▶│TimescaleDB│◀──│  Grafana    │  │
│  │(数据拉取) │   │(时序存储) │   │ (可视化)    │  │
│  └──────────┘   └──────────┘   └─────────────┘  │
│       │               ▲                          │
│  ┌────▼─────┐   ┌─────┴────┐                    │
│  │  Polars  │   │  Redis   │                    │
│  │(数据处理) │   │ (缓存)   │                    │
│  └──────────┘   └──────────┘                    │
│       │                                          │
│  ┌────▼─────┐   ┌──────────┐                    │
│  │  Qlib    │   │JupyterLab│                    │
│  │(策略框架) │   │ (研究)   │                    │
│  └──────────┘   └──────────┘                    │
└─────────────────────────────────────────────────┘
```

## 服务端口

| 服务         | 端口  | 说明               |
|------------|-------|------------------|
| TimescaleDB | 5432  | PostgreSQL 兼容     |
| Redis       | 6379  | 缓存               |
| JupyterLab  | 8888  | 研究环境             |
| Grafana     | 3000  | 监控大盘             |

## 快速开始

### 1. 初始化配置

```bash
cp .env.example .env
# 编辑 .env，填写 TUSHARE_TOKEN 及其他密码
```

### 2. 构建并启动

```bash
# 首次构建（需要下载依赖，约 5~10 分钟）
docker compose build

# 启动所有服务
docker compose up -d

# 查看日志
docker compose logs -f
```

### 3. 验证服务状态

```bash
docker compose ps
```

### 4. 访问 JupyterLab

浏览器打开：http://localhost:8888
Token 见 `.env` 中的 `JUPYTER_TOKEN`

打开 `notebooks/quick_start/01_quick_start.ipynb` 开始体验。

### 5. 访问 Grafana

浏览器打开：http://localhost:3000
账号密码见 `.env` 中的 `GRAFANA_USER` / `GRAFANA_PASSWORD`

## 常用命令

```bash
# 进入 app 容器
docker compose exec app bash

# 初始化 Qlib 数据
docker compose exec app bash scripts/init_qlib_data.sh

# 连接数据库
docker compose exec timescaledb psql -U quant -d quant_db

# 查看超表信息
docker compose exec timescaledb psql -U quant -d quant_db \
  -c "SELECT * FROM timescaledb_information.hypertables;"

# 停止服务（保留数据）
docker compose down

# 停止并清除所有数据（谨慎！）
docker compose down -v
```

## 每日 ETL

```bash
# 每日增量更新（默认检查最近 7 个自然日，自动补全孔洞）
docker compose exec app bash scripts/etl_daily.sh

# 每周对账：检查最近 30 天
docker compose exec app bash scripts/etl_daily.sh --lookback-days 30

# 查看同步状态
docker compose exec timescaledb psql -U quant -d quant_db \
  -c "SELECT data_type, last_date, status, error_msg, updated_at FROM meta.sync_status;"
```

### Cron 配置（宿主机，A 股收盘后 17:30 CST = 09:30 UTC）

```cron
# 每个交易日增量更新
30 9 * * 1-5  docker compose -f /path/to/docker-compose.yml exec -T app bash scripts/etl_daily.sh

# 每周日对账（30 天回溯）
0 20 * * 0    docker compose -f /path/to/docker-compose.yml exec -T app bash scripts/etl_daily.sh --lookback-days 30
```

### ETL 孔洞检测逻辑

每次运行自动对比 `market.daily` 中已有日期与交易日历，仅拉取缺失日期：
- 正常运行：补齐当日数据
- API 超时/限频导致漏拉：下次运行自动回填
- 运行结果写入 `meta.sync_status`（`status='ok'` 或 `'error'`）

### 日志

ETL 日志写入 `logs/` 目录（挂载至容器内 `/app/logs`），按时间戳命名：

```
logs/
  etl_daily_20260317_093000.log
  etl_daily_20260318_093001.log
  ...
```

```bash
# 查看最新 ETL 日志
tail -f logs/$(ls -t logs/ | head -1)
```

## 项目结构

```
.
├── docker-compose.yml          # 服务编排
├── Dockerfile                  # Python 应用镜像
├── requirements.txt            # Python 依赖
├── .env.example                # 环境变量模板
│
├── docker/
│   ├── timescaledb/
│   │   └── init.sql            # 数据库初始化 (超表/索引/视图)
│   └── grafana/
│       └── provisioning/       # Grafana 数据源预配置
│
├── app/
│   ├── data_pipeline/
│   │   └── fetch_daily.py      # Tushare 日线数据拉取 → market.daily
│   ├── factors/
│   │   ├── base.py             # BaseFactor 抽象基类
│   │   ├── technical.py        # MA/RSI/MACD 等技术因子 (ta 库)
│   │   └── pipeline.py         # 批量计算因子 → factors.daily_factors
│   ├── strategy/
│   │   ├── base.py             # BaseStrategy 抽象基类
│   │   └── momentum.py         # 动量策略示例 (MA金叉 + RSI)
│   ├── backtest/
│   │   ├── runner.py           # Qlib 回测入口
│   │   └── metrics.py          # Sharpe / 最大回撤 / Calmar 等指标
│   └── utils/
│       ├── db.py               # 数据库连接工具 (SQLAlchemy 单例)
│       ├── signals.py          # 信号写入 → signals.trading_signals
│       └── qlib_helper.py      # Qlib 初始化工具
│
├── notebooks/
│   └── quick_start/
│       ├── 01_quick_start.ipynb    # 快速入门：数据拉取与写库
│       ├── 02_factor_research.ipynb # 因子计算与 IC 分析
│       └── 03_backtest.ipynb       # 策略信号生成与回测绩效
│
├── logs/                       # ETL 运行日志（挂载至容器 /app/logs）
│
├── scripts/
│   ├── etl_daily.sh            # 每日增量 ETL (孔洞检测 + 自动补全)
│   ├── etl_daily.py            # ETL 主逻辑 (Python)
│   ├── init_qlib_data.sh       # Qlib 数据初始化 (一次性)
│   └── run_factor_pipeline.sh  # 批量运行因子流水线
│
└── config/
    └── strategies/
        └── momentum.yaml       # 动量策略超参数配置
```

## 数据库 Schema

```
quant_db
├── meta
│   ├── stocks              # 股票基础信息
│   └── sync_status         # 数据同步状态
├── market
│   ├── daily               # 日线行情 (超表)
│   ├── minute              # 分钟线行情 (超表)
│   ├── index_daily         # 指数日线 (超表)
│   ├── weekly              # 周线 (连续聚合视图)
│   └── monthly             # 月线 (连续聚合视图)
├── factors
│   └── daily_factors       # 因子数据 (超表)
└── signals
    └── trading_signals     # 交易信号 (超表)
```
