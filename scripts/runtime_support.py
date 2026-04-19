from pathlib import Path

from dotenv import load_dotenv

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
_ENV_FILES = (
    PROJECT_ROOT / ".env",
    SCRIPTS_DIR / ".env",
)


def load_project_dotenv() -> None:
    for env_path in _ENV_FILES:
        if env_path.exists():
            load_dotenv(env_path, override=False)


def resolve_repo_path(path_str: str | Path) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else PROJECT_ROOT / path


def resolve_optional_repo_path(path_str: str | Path | None) -> Path | None:
    return None if path_str is None else resolve_repo_path(path_str)


def ensure_parent_dir(path_str: str | Path) -> Path:
    path = resolve_repo_path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
