import json
from pathlib import Path



CONFIG_DIR = Path(__file__).parent


def load_sensitive_columns():
    config_file = CONFIG_DIR / "sensitive_columns.json"

    with open(config_file,"r") as f:
        data = json.load(f)

    return set(data.get("global_sensitive_columns", []))