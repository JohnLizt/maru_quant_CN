# ─────────────────────────────────────────────────────────────
# A股量化系统 - Python 应用镜像
# 包含: AKshare | Polars | Qlib | JupyterLab
# ─────────────────────────────────────────────────────────────
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim-bookworm

# 构建参数
ARG DEBIAN_FRONTEND=noninteractive

# ── 系统依赖 ─────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 编译工具 (Qlib / 部分 C 扩展需要)
    build-essential \
    gcc \
    g++ \
    # 数据库驱动
    libpq-dev \
    # 数值计算
    libopenblas-dev \
    liblapack-dev \
    # 网络 / 证书
    curl \
    ca-certificates \
    # 时区
    tzdata \
    # Git (Qlib 安装需要)
    git \
    && rm -rf /var/lib/apt/lists/*

# ── 时区 ────────────────────────────────────────────────────
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ── Python 环境 ──────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ── 先安装依赖 (利用 Docker 层缓存) ──────────────────────────
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# ── 复制应用代码 ──────────────────────────────────────────────
COPY app/ ./app/
COPY config/ ./config/
COPY scripts/ ./scripts/
COPY notebooks/ ./notebooks/

# ── 创建数据目录 ──────────────────────────────────────────────
RUN mkdir -p /app/qlib_data /app/data /app/logs

# ── 非 root 用户 (可选，提升安全性) ──────────────────────────
# RUN useradd -m -u 1000 quant && chown -R quant:quant /app
# USER quant

EXPOSE 8888

# 默认入口：保持容器运行 (由 docker-compose command 覆盖)
CMD ["tail", "-f", "/dev/null"]
