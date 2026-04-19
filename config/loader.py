"""Load YAML/JSON configuration."""
from pathlib import Path
from typing import Any

import yaml


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load config from YAML or JSON file."""
    if path is None:
        path = Path(__file__).parent / "settings.yaml"
    path = Path(path)
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        if path.suffix in (".yaml", ".yml"):
            return yaml.safe_load(f) or {}
        if path.suffix == ".json":
            import json
            return json.load(f)
    return {}


def get_settings(key_path: str = "", default: Any = None) -> Any:
    """Get nested config value by dot path (e.g. 'app.request_delay_min')."""
    config = load_config()
    if not key_path:
        return config
    keys = key_path.split(".")
    value = config
    for k in keys:
        value = value.get(k) if isinstance(value, dict) else None
        if value is None:
            return default
    return value
