from pathlib import Path

import pytest
import yaml

from incremental_replay.scenario import ReplayError, load_scenario

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def fixture():
    base = EXAMPLES / "late-correction"
    data = yaml.safe_load((base / "fixed.yml").read_bytes())
    data["project"] = str(EXAMPLES / "orders")
    for step in data["steps"][:2]:
        step["snapshots"]["raw.orders"] = str(base / step["snapshots"]["raw.orders"])
    return data


def test_retry_reuses_identical_snapshot_and_clock(tmp_path):
    data = fixture()
    data["checkpoints"] = ["late correction"]
    path = tmp_path / "scenario.yml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    scenario = load_scenario(path)
    assert scenario.checkpoints == {"late correction"}
    assert scenario.steps[-1].snapshots == scenario.steps[-2].snapshots
    assert scenario.steps[-1].variables == scenario.steps[-2].variables


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda d: d["steps"][0].update(retry=True), "retry must reuse"),
        (lambda d: d["steps"][1].update(snapshots={}), "complete snapshot"),
        (lambda d: d.update(checkpoints=["missing"]), "checkpoints"),
        (lambda d: d.update(key=["order_id", "ORDER_ID"]), "distinct"),
    ],
)
def test_rejects_ambiguous_scenario(tmp_path, mutation, message):
    data = fixture()
    mutation(data)
    path = tmp_path / "scenario.yml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    with pytest.raises(ReplayError, match=message):
        load_scenario(path)
