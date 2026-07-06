"""
Guards against a common production incident: an ADF DatabricksNotebook activity passes
base parameters that a notebook silently ignores if a widget name typo is introduced.
This script scans notebook source for dbutils.widgets.get() calls and cross-checks them
against the baseParameters keys referenced in the corresponding ADF pipeline JSON.
"""
import json
import re
import sys
from pathlib import Path

NOTEBOOK_DIR = Path("notebooks")
PIPELINE_DIR = Path("adf/pipeline")


def get_widget_names(notebook_path: Path) -> set[str]:
    text = notebook_path.read_text()
    return set(re.findall(r'dbutils\.widgets\.get\("([^"]+)"\)', text))


def get_pipeline_param_refs(pipeline_json: dict) -> set[str]:
    text = json.dumps(pipeline_json)
    return set(re.findall(r'"([a-zA-Z_]+)":\s*"@pipeline\(\)\.parameters', text))


def main() -> int:
    errors = []
    for pipeline_file in PIPELINE_DIR.glob("*.json"):
        pipeline = json.loads(pipeline_file.read_text())
        for activity in pipeline.get("properties", {}).get("activities", []):
            if activity.get("type") == "DatabricksNotebook":
                nb_path = Path(activity["typeProperties"]["notebookPath"]).name
                nb_file = NOTEBOOK_DIR / f"{nb_path}.py"
                if not nb_file.exists():
                    continue
                widget_names = get_widget_names(nb_file)
                base_params = set(activity["typeProperties"].get("baseParameters", {}).keys())
                missing = base_params - widget_names
                if missing:
                    errors.append(f"{pipeline_file.name} -> {nb_file.name}: params {missing} not read via dbutils.widgets.get()")

    if errors:
        print("Notebook/pipeline contract mismatches found:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("All notebook <-> ADF pipeline parameter contracts are consistent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
