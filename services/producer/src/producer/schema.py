from pathlib import Path

# Path from this file up to the repo-root `schemas/` directory.
SCHEMA_PATH = Path(__file__).parents[4] / "schemas" / "user_created.avsc"


def load_schema_str() -> str:
    return SCHEMA_PATH.read_text()
