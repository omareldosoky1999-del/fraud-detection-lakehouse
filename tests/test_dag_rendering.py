"""Render every BashOperator command in the DAGs with Jinja.

Airflow is intentionally not a CI dependency, so DAG files are parsed with
``ast`` and each ``bash_command`` expression is evaluated and rendered the same
way Airflow would. This catches template bugs (e.g. a missing ``f`` prefix
leaving ``{{{{ ds }}}}`` in the string) that substring assertions cannot.
"""
import ast
from pathlib import Path

import pytest
from jinja2 import Environment, StrictUndefined

DAG_DIR = Path(__file__).resolve().parents[1] / "orchestration" / "dags"
DAG_FILES = sorted(p for p in DAG_DIR.glob("*.py") if p.name != "__init__.py")


def _bash_commands(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            try:
                constants[node.targets[0].id] = ast.literal_eval(node.value)
            except Exception:
                pass
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "BashOperator":
            task_id, command = None, None
            for kw in node.keywords:
                if kw.arg == "task_id":
                    task_id = ast.literal_eval(kw.value)
                elif kw.arg == "bash_command":
                    command = eval(compile(ast.Expression(kw.value), str(path), "eval"), dict(constants))
            if command is not None:
                found.append((task_id, command))
    return found


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.name)
def test_bash_commands_render(path):
    env = Environment(undefined=StrictUndefined)
    commands = _bash_commands(path)
    assert commands, f"no BashOperator commands found in {path.name}"
    for task_id, command in commands:
        rendered = env.from_string(command).render(
            ds="2026-09-29",
            var={"value": {"get": lambda key, default=None: default}},
        )
        assert "{{" not in rendered and "}}" not in rendered, (task_id, rendered)


def test_gold_dag_passes_logical_date():
    env = Environment(undefined=StrictUndefined)
    rendered = {
        task_id: env.from_string(cmd).render(ds="2026-09-29", var={"value": {"get": lambda k, d=None: d}})
        for task_id, cmd in _bash_commands(DAG_DIR / "fraud_gold_dag.py")
    }
    assert "--date 2026-09-29" in rendered["validate_silver_quality"]
    assert "--date 2026-09-29" in rendered["spark_submit_build_gold"]
    assert "silver_validation_2026-09-29.json" in rendered["validate_silver_quality"]
