import yaml
from pathlib import Path
from typing import Any, Dict, Optional

_config: Optional[Dict[str, Any]] = None


def load_config() -> Dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    config_file = repo_root / "config.yaml"

    if config_file.exists():
        print(f"✅ configuration loaded from: {config_file}")
        with open(config_file, "r") as f:
            data: Dict[str, Any] = yaml.safe_load(f)
            return data
    else:
        raise FileNotFoundError(f"Config file not found at: {config_file}")


def get_config() -> Dict[str, Any]:
    global _config
    if _config is None:
        _config = load_config()
    return _config
