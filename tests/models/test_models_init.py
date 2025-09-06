# tests/models/test_models_init.py

import posms.models as m


def test_models_lazy_exports_and_version():
    # 遅延 re-export が公開されていること
    assert "ModelTrainer" in dir(m)
    assert "ModelPredictor" in dir(m)

    # __version__ は常に文字列（例: "0.1.0" または "0+unknown"）
    assert isinstance(m.__version__, str) and len(m.__version__) > 0


def test_models_lazy_exports_are_cached():
    # 1回目のアクセスで解決
    trainer1 = getattr(m, "ModelTrainer")
    predictor1 = getattr(m, "ModelPredictor")

    # __dict__ にキャッシュされていること
    assert "ModelTrainer" in m.__dict__
    assert "ModelPredictor" in m.__dict__

    # 2回目は同一オブジェクト参照（再解決されない）
    trainer2 = getattr(m, "ModelTrainer")
    predictor2 = getattr(m, "ModelPredictor")
    assert trainer1 is trainer2
    assert predictor1 is predictor2
