__all__ = ["run_backtest", "run_strategy_backtest", "compute_metrics"]


def run_backtest(*args, **kwargs):
    from app.backtest.runner import run_backtest as _run_backtest

    return _run_backtest(*args, **kwargs)


def run_strategy_backtest(*args, **kwargs):
    from app.backtest.runner import run_strategy_backtest as _run_strategy_backtest

    return _run_strategy_backtest(*args, **kwargs)


def compute_metrics(*args, **kwargs):
    from app.backtest.metrics import compute_metrics as _compute_metrics

    return _compute_metrics(*args, **kwargs)
