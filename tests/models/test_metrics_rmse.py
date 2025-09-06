# tests/test_metrics_rmse.py
import numpy as np
from posms.models import _metrics as M


def test_rmse_basic_and_kwargs():
    y = np.array([1.0, 2.0, 3.0])
    assert M.rmse(y, y) == 0.0
    assert M.rmse(y, y, sample_weight=[1, 1, 1]) == 0.0
    assert M.rmse(y, y, multioutput="uniform_average") == 0.0


def test_rmse_weighted():
    y = np.array([0.0, 0.0, 10.0])
    yhat = np.array([0.0, 5.0, 10.0])
    # 非重み付き
    expected = float(np.sqrt(np.mean((y - yhat) ** 2)))
    assert np.isclose(M.rmse(y, yhat), expected)
    # 重み付き（2番目を重く）
    w = np.array([1.0, 3.0, 1.0])
    expected_w = float(np.sqrt(np.average((y - yhat) ** 2, weights=w)))
    assert np.isclose(M.rmse(y, yhat, sample_weight=w), expected_w)


def test_rmse_multioutput_raw_values_is_averaged_to_float():
    # 2 出力の RMSE を raw_values で出した場合でも、M.rmse は平均して float を返す仕様
    y = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
    yhat = y + np.array([[1.0, 0.0], [0.0, -1.0], [1.0, 1.0]])
    rmse1 = float(np.sqrt(np.mean((y[:, 0] - yhat[:, 0]) ** 2)))
    rmse2 = float(np.sqrt(np.mean((y[:, 1] - yhat[:, 1]) ** 2)))
    expected_avg = float(np.mean([rmse1, rmse2]))
    got = M.rmse(y, yhat, multioutput="raw_values")
    assert isinstance(got, float)
    assert np.isclose(got, expected_avg)


def test_rmse_forced_fallback_path(monkeypatch):
    # root_mean_squared_error が無い環境のフォールバック経路を強制テスト
    monkeypatch.setattr(M, "_rmse", None, raising=False)
    y = np.array([1.0, 2.0, 3.0])
    yhat = np.array([2.0, 2.0, 2.0])
    expected = float(np.sqrt(np.mean((y - yhat) ** 2)))
    assert np.isclose(M.rmse(y, yhat), expected)
