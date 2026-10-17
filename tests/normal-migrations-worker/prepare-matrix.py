"""Build the DO oracle from exact writer checkpoints, preserving raw Piece/Page JSON."""

import base64
import json
import os
import pathlib
import sys

root = pathlib.Path.cwd()
destination = pathlib.Path(sys.argv[1]).resolve()
destination.parent.mkdir(parents=True, exist_ok=True)


def relative(path):
    return os.path.relpath(path, destination.parent)


def read(path):
    return json.loads(path.read_text())


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def checkpoint(record):
    queries = []
    expected = []
    for table, rows in sorted(record["typedRows"].items()):
        columns = record["columns"][table]
        projection = []
        for name in columns:
            column = quote(name)
            projection += [
                f"typeof({column})",
                f"CASE WHEN typeof({column})='integer' THEN CAST({column} AS TEXT) WHEN typeof({column})='blob' THEN hex({column}) ELSE {column} END",
            ]
        queries.append(
            "SELECT json_array("
            + ",".join(projection)
            + ") AS result FROM "
            + quote(table)
            + " ORDER BY "
            + quote(columns[0])
        )
        values = []
        for row in rows:
            typed = []
            for cell in row:
                kind = cell["kind"]
                value = cell.get("value")
                if kind == "integer":
                    value = str(value)
                if kind == "blob":
                    value = base64.b64decode(value).hex().upper()
                typed += [kind, value]
            values.append(typed)
        expected.append(values)
        name_literal = "'" + table.replace("'", "''") + "'"
        queries.append(
            "SELECT json_group_array(name) AS result FROM (SELECT name FROM pragma_table_info("
            + name_literal
            + ") ORDER BY cid)"
        )
        expected.append([columns])
    schema = record["schemaIdentity"]
    position = record["position"]
    queries.append(
        "SELECT json_array(json_extract(record,'$.copy.schema.version'),json_extract(record,'$.copy.schema.fingerprint'),json_extract(record,'$.copy.acked.epoch'),json_extract(record,'$.copy.acked.sequence')) AS result FROM _fleet_sync_copy WHERE singleton=1"
    )
    expected.append(
        [
            [
                schema["version"],
                schema["fingerprint"],
                position["epoch"],
                position["sequence"],
            ]
        ]
    )
    return {"name": record["name"], "queries": queries, "expected": expected}


base = root / (sys.argv[2] if len(sys.argv) > 2 else "normal-evidence/fixtures")
selected = {
    path.name: path
    for path in base.iterdir()
    if path.is_dir() and (path / "create-copy.json").exists()
}
for category in (
    [] if len(sys.argv) > 2 else ["fixtures-sensitive", "fixtures-sensitive-cold"]
):
    parent = root / "normal-evidence" / category
    if parent.exists():
        for path in parent.iterdir():
            if path.is_dir() and (path / "create-copy.json").exists():
                selected[path.name] = path
cases = []
snapshots = []
for name, path in sorted(selected.items()):
    pieces = sorted(path.glob("piece-*.json"))
    checkpoints = {}
    records = {}
    for source in path.glob("writer-checkpoint-*.json"):
        records[read(source)["name"]] = read(source)
    for source in path.glob("checkpoint-*.json"):
        records[read(source)["name"]] = read(source)
    for record in records.values():
        matches = [
            index + 1
            for index, piece in enumerate(pieces)
            if read(piece)["through"] == record["position"]
            and (
                read(piece).get("part") is None
                or read(piece)["part"]["index"] + 1 == read(piece)["part"]["count"]
            )
        ]
        if matches:
            checkpoints[str(matches[-1])] = checkpoint(record)
    status = (
        read(path / "case-status.json")
        if (path / "case-status.json").exists()
        else {"status": "unknown"}
    )
    cases.append(
        {
            "name": name,
            "create": read(path / "create-copy.json"),
            "sender": read(path / "sender.json"),
            "pieces": [relative(piece) for piece in pieces],
            "checkpoints": checkpoints,
            "targetProduced": status["status"] == "passed",
            "writerStatus": status,
        }
    )
    offline = path / "offline-stream"
    if offline.exists():
        off_pieces = sorted(offline.glob("piece-*.json"))
        record = read(next(offline.glob("checkpoint-*.json")))
        cases.append(
            {
                "name": name + "-offline",
                "create": read(offline / "create-copy.json"),
                "sender": read(offline / "sender.json"),
                "pieces": [relative(piece) for piece in off_pieces],
                "checkpoints": {str(len(off_pieces)): checkpoint(record)},
                "targetProduced": True,
                "writerStatus": status,
            }
        )
    if (path / "snapshot-manifest.json").exists():
        manifest = read(path / "snapshot-manifest.json")
        source = read(path / "source-copy-status.json")
        install = {
            "install_id": "019eef00-0000-7000-8000-000000000099",
            "database": manifest["database"],
            "position": manifest["position"],
            "schema": manifest["schema"],
        }
        pages = []
        cursor = None
        for index, page_path in enumerate(sorted(path.glob("snapshot-page-*.json"))):
            pages.append(
                {"index": index, "path": relative(page_path), "cursor": cursor}
            )
            cursor = read(page_path)["next"]
        last = max(records.values(), key=lambda record: record["position"]["sequence"])
        snapshots.append(
            {
                "name": name + "-new-snapshot",
                "install": install,
                "source": source,
                "pages": pages,
                "manifest": manifest,
                "checkpoint": checkpoint(last),
            }
        )
output = {"cases": cases, "snapshots": snapshots}
destination.write_text(json.dumps(output, indent=2) + "\n")
print(
    json.dumps(
        {
            "cases": len(cases),
            "checkpoints": sum(len(case["checkpoints"]) for case in cases),
            "snapshots": len(snapshots),
            "writerBlocked": [
                case["name"] for case in cases if not case["targetProduced"]
            ],
        }
    )
)
