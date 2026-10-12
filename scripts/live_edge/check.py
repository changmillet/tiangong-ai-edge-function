#!/usr/bin/env python3
"""Bounded HTTP checks of actual deployed handlers on the synthetic preview.

No provider SDK/mock imports. Raw responses stay in private receipts outside Git.
Cold invalid credentials and accepted hot email-cache behavior are distinct tests.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener

try:
    from .fixtures import NoRedirect, REPO, SafeError, load_private, private_json, validate_manifest
except ImportError:
    from fixtures import NoRedirect, REPO, SafeError, load_private, private_json, validate_manifest

HANDLERS = {"edu": "edu_search", "esg": "esg_search", "standard": "standard_search", "sci": "sci_search"}
PHASE_REQUEST_LIMITS = {"edu": 5, "esg": 3, "standard": 3, "sci": 2}


def phase_sources(source: str | None = None) -> tuple[str, ...]:
    if source is not None and source not in HANDLERS:
        raise SafeError("Unsupported qualification source")
    return (source,) if source else tuple(HANDLERS)


class RequestBudget:
    def __init__(self, target: dict, output: Path, request_limit: int = 16):
        self.target = target
        self.output = output
        self.request_limit = request_limit
        self.deadline = time.monotonic() + 180
        self.calls = []
        self.outcomes = []

    def request(self, name: str, body: dict, user: dict, *, wrong_password=False):
        if len(self.calls) >= self.request_limit or time.monotonic() >= self.deadline:
            raise SafeError("Live handler request/deadline budget exhausted")
        if name not in ("edu_search", "esg_search", "standard_search", "sci_search"):
            raise SafeError("Handler is outside the test allowlist")
        timeout = min(40, max(1, self.deadline - time.monotonic()))
        headers = {"apikey": self.target["anon_key"], "Authorization": "Bearer " + self.target["anon_key"], "Content-Type": "application/json", "email": user["email"], "password": "cold-synthetic-invalid-password" if wrong_password else user["password"]}
        request = Request(self.target["api_url"].rstrip("/") + "/functions/v1/" + name, method="POST", headers=headers, data=json.dumps(body).encode())
        call = {"handler": name, "case": body.get("action", "search"), "wrong_password": wrong_password}
        self.calls.append(call)
        try:
            with build_opener(NoRedirect()).open(request, timeout=timeout) as response:
                status, raw = response.status, response.read(2_000_001)
        except HTTPError as error:
            status, raw = error.code, error.read(2_000_001)
        except (URLError, TimeoutError):
            call["transport_failed"] = True
            self.save()
            raise SafeError("Live handler transport failed; diagnostics withheld") from None
        if len(raw) > 2_000_000:
            raise SafeError("Handler response exceeds the receipt budget")
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeError):
            data = raw.decode("utf-8", errors="replace")
        call.update({"status": status, "response": data})
        self.save()
        return status, data

    def save(self):
        private_json(self.output, {"project_ref": self.target["project_ref"], "run_id": self.target["run_id"], "calls": self.calls, "outcomes": self.outcomes})

    def require(self, condition: bool, outcome: str):
        self.outcomes.append({"check": outcome, "passed": bool(condition)})
        self.save()
        if not condition:
            raise SafeError("Live handler check failed: " + outcome)


def check(target_path: Path, source: str | None = None) -> dict:
    selected = phase_sources(source)
    target = load_private(target_path)
    validate_manifest(target)
    if target.get("fixture_state") != "ready":
        raise SafeError("Live check requires the ready synthetic fixture manifest")
    scope = source or "all"
    request_limit = PHASE_REQUEST_LIMITS[source] if source else 16
    budget = RequestBudget(target, target_path.parent / f"deployed-handler-results.{scope}.private.json", request_limit)
    f = target["fixtures"]
    query = "Synthetic retrieval " + f["marker"]
    common = {"query": query, "topK": 1, "extK": 1}
    for selected_source in selected:
        name = HANDLERS[selected_source]
        status, body = budget.request(name, {**common, "getMeta": False}, target["outsider"], wrong_password=True)
        # All four actual handlers return this exact body after failed password
        # authentication. A gateway/JWT rejection is not a handler auth proof.
        budget.require(status == 401 and body == "Unauthorized", name + "_cold_handler_invalid_credentials_401")
    if "edu" in selected:
        status, facets = budget.request("edu_search", {"action": "list_filter_options", "fields": ["course", "name", "chapter_number"], "filter": {"course": [f["edu_course"]]}}, target["user"])
        budget.require(status == 200 and isinstance(facets, dict) and facets.get("matched_records") == 1 and {item["value"] for item in facets.get("options", {}).get("course", [])} == {f["edu_course"]} and {item["value"] for item in facets.get("options", {}).get("name", [])} == {f["edu_name"]}, "edu_real_session_facets")
        status, _ = budget.request("edu_search", {"action": "list_filter_options", "fields": ["unsupported_synthetic_field"]}, target["user"])
        budget.require(status == 400, "edu_invalid_facet_field_400")

    def require_retrieval(status, rows, label):
        budget.require(status == 200 and isinstance(rows, list) and bool(rows) and all(isinstance(row, dict) and isinstance(row.get("content"), str) and isinstance(row.get("source"), str) for row in rows), label + "_response_shape")
        content = "\n".join(row["content"] for row in rows)
        for marker in ("PINECONE_ONLY_", "OPENSEARCH_ONLY_", "EXTENSION_"):
            budget.require(marker + target["run_id"] in content, label + "_" + marker.lower().strip("_"))
        return rows

    if "edu" in selected:
        status, rows = budget.request("edu_search", {**common, "filter": {"course": [f["edu_course"]]}}, target["user"])
        require_retrieval(status, rows, "edu")
        budget.require(all(f["edu_course"] in row["source"] and f["edu_name"] in row["source"] for row in rows), "edu_synthetic_source")
        # EDU cached retrieval deliberately accepts a changed password once the
        # exact synthetic email was cached by a successful login. No metadata-login
        # path is involved; do not mislabel this established behavior as rejection.
        status, rows = budget.request("edu_search", {**common, "filter": {"course": [f["edu_course"]]}}, target["user"], wrong_password=True)
        require_retrieval(status, rows, "edu_hot_cache")
        budget.require(status == 200, "email_cache_hot_wrong_password_authorized_as_designed")
    for name, label in (("esg_search", "esg"), ("standard_search", "standard")):
        if label not in selected:
            continue
        status, rows = budget.request(name, {**common, "meta_contains": f["marker"]}, target["user"])
        require_retrieval(status, rows, label + "_metadata_match")
        budget.require(all(f["marker"] in row["source"] for row in rows), label + "_synthetic_source")
        status, empty = budget.request(name, {**common, "meta_contains": "nomatch" + target["run_id"] + "neverexists"}, target["user"])
        budget.require(status == 200 and isinstance(empty, dict) and empty.get("message") == "No records found matching the metadata filters.", label + "_metadata_nomatch")
    if "sci" in selected:
        status, rows = budget.request("sci_search", {**common, "getMeta": True, "filter": {"doi": [f["journal_doi"]]}}, target["user"])
        require_retrieval(status, rows, "sci")
        budget.require(all(f["journal_doi"] in row["source"] and f["journal_title"] in row["source"] and all(author in row["source"] for author in f["journal_authors"]) for row in rows), "sci_actual_postgres_metadata_hydration")
    hashes = {}
    for path in sorted((REPO / "supabase/functions").rglob("*.ts")):
        if path.parent.name == "_shared" or path.parent.name in {HANDLERS[item] for item in selected}:
            hashes[path.relative_to(REPO).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    result = {"ok": True, "source": scope, "sources": list(selected), "project_ref": target["project_ref"], "run_id": target["run_id"], "request_count": len(budget.calls), "request_limit": request_limit, "deadline_seconds": 180, "checks": budget.outcomes, "expected_checkout_source_sha256": hashes, "deployed_source_sha256": target.get("deployed_source_sha256"), "raw_responses": "Private receipt only; no production business data read/exported"}
    private_json(target_path.parent / f"deployed-handler-summary.{scope}.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--source", choices=tuple(HANDLERS), help="Qualify only this consumer; omitted runs all four.")
    arguments = parser.parse_args()
    try:
        result = check(arguments.target, arguments.source)
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "error": str(error) if isinstance(error, SafeError) else "Handler validation failed; response and credentials withheld."}))
        raise SystemExit(1)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
