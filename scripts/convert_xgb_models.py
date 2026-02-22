from pathlib import Path
import xgboost as xgb

ROOT = Path("model_bundle")

for kind_dir in ROOT.iterdir():
    if not kind_dir.is_dir():
        continue
    src = kind_dir / "model.xgb"
    if not src.exists():
        continue

    booster = xgb.Booster()
    booster.load_model(str(src))

    # JSON形式（推奨）
    dst_json = kind_dir / "model.json"
    booster.save_model(str(dst_json))

    # UBJ形式（さらに新しい推奨。xgboost 1.7 で保存できない場合もあるのでその時は json だけでOK）
    dst_ubj = kind_dir / "model.ubj"
    try:
        booster.save_model(str(dst_ubj))
    except Exception as e:
        print(f"[WARN] cannot save UBJ for {kind_dir.name}: {e}")

    print(f"converted: {src} -> {dst_json} (and maybe .ubj)")