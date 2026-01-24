# tests/models/test_models_init.py

import importlib


def test_models_is_namespace_package():
    # posms.models は “モデル群の名前空間” として存在する
    m = importlib.import_module("posms.models")
    assert hasattr(m, "__path__")  # package であること（サブパッケージを持てる）


def test_normal_model_public_api_exists():
    # normal が import できること
    importlib.import_module("posms.models.normal")

    # normal の公開クラスが存在すること
    trainer_mod = importlib.import_module("posms.models.normal.trainer")
    predictor_mod = importlib.import_module("posms.models.normal.predictor")

    assert hasattr(trainer_mod, "ModelTrainer")
    assert hasattr(predictor_mod, "ModelPredictor")
