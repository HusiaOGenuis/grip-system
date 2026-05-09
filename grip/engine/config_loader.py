from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"


class ConfigurationError(Exception):
    """Raised when governance configuration is invalid or missing."""


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigurationError(f"Missing configuration file: {path.name}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ConfigurationError(f"Invalid YAML structure in {path.name}")

    return data


def load_verdict_taxonomy() -> dict:
    data = _load_yaml(CONFIG_DIR / "verdict_taxonomy.yaml")

    verdicts = data.get("verdicts")
    if not verdicts or not isinstance(verdicts, list):
        raise ConfigurationError("Verdict taxonomy must define a list of verdicts")

    return data


def load_pause_criteria() -> dict:
    data = _load_yaml(CONFIG_DIR / "pause_criteria.yaml")

    criteria = data.get("criteria")
    if not criteria or not isinstance(criteria, list):
        raise ConfigurationError("Pause criteria must define a list of criteria")

    return data


def load_all_configuration() -> dict:
    return {
        "verdict_taxonomy": load_verdict_taxonomy(),
        "pause_criteria": load_pause_criteria(),
    }
