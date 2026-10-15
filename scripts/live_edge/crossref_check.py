#!/usr/bin/env python3
"""Bounded real public Crossref collection into the fixed data-free preview.

The existing collector and skills remain byte-identical. A transport boundary
caps the first real journal page to two articles and requests a real rows=0
terminal page. The duplicate run replays those captured HTTP responses as an
explicit local request cache. No provider response is synthesized or published.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime, timezone
import hashlib
import html
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
from urllib.parse import quote, urlsplit
from urllib.request import build_opener
import uuid

from fixtures import BRANCH_REF, NoRedirect, REPO, SafeError, load_private, private_json, validate_target

WORKSPACE = REPO.parent
ISSN = "1932-6203"
JOURNAL = "PLOS ONE"
MAX_PAPERS = 2
MAX_REQUESTS = 4
MAX_BYTES = 2_000_000
POSITIVE_SAMPLE = {
    "10.1128/jcm.12.1.60-62.1980": {
        "journal": "Journal of Clinical Microbiology", "length": 1411,
        "sha256": "cbb529e80d200eff7440d7e884cdcdc9beb6b0bfc0ee1407a0d8a0f12280f7d2",
    },
    "10.31168/2305-6754.2023.1.05": {
        "journal": "Slovene", "length": 1519,
        "sha256": "9f4810a6946e4e8f247bf35d9fce522269ed16778f7ccd2ae0e49c0807992b66",
    },
}
POSITIVE_MAX_REQUESTS = 4  # Two fresh metadata GETs and two fresh resolver GETs.


def qualification_sources(workspace: Path | None = None) -> tuple[Path, Path]:
    """Locate source checkouts in canonical or pre-rename workspaces."""
    workspace = WORKSPACE if workspace is None else workspace

    def sibling(canonical: str, legacy: str) -> Path:
        for name in (canonical, legacy):
            candidate = workspace / name
            if candidate.is_dir():
                return candidate
        raise SafeError(f"Required sibling checkout is missing: {canonical}")

    return sibling("knowledge-base", "TianGong-AI-KB"), sibling("agent-skills", "skills")


class TransportStop(BaseException):
    """Escape the skill's 30-attempt retry policy without another network call."""


def read_state() -> dict:
    return json.loads(Path(os.environ["CROSSREF_CHECK_STATE"]).read_text())


def write_state(state: dict) -> None:
    private_json(Path(os.environ["CROSSREF_CHECK_STATE"]), state)


def install_transport() -> None:
    """Child bootstrap: real requests Session, bounded paging, recorded replay."""
    import requests

    mode = os.environ["CROSSREF_CHECK_MODE"]
    page_count = 0
    original_session = requests.Session

    def get(url, params=None, **kwargs):
        nonlocal page_count
        if url != f"https://api.crossref.org/journals/{ISSN}/works" or page_count >= 2:
            raise TransportStop("Crossref collector transport budget exhausted")
        page_count += 1
        slot = str(page_count)
        state = read_state()
        if mode == "record":
            if len(state["requests"]) >= MAX_REQUESTS:
                raise TransportStop("Crossref request limit exhausted")
            bounded = dict(params or {})
            bounded["rows"] = MAX_PAPERS if page_count == 1 else 0
            if page_count == 2:
                bounded.pop("cursor", None)
            state["requests"].append({"source": "Crossref public journal metadata", "rows": bounded["rows"], "page": page_count})
            write_state(state)
            session = original_session()
            session.trust_env = False
            try:
                response = session.get(url, params=bounded, timeout=(10, 20), stream=True, allow_redirects=False, headers={"User-Agent": "Tiangong-Isolated-KB-Qualification/1.0", "Accept": "application/json"})
                if response.status_code != 200:
                    raise TransportStop("Crossref did not return HTTP 200")
                chunks, size = [], 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > MAX_BYTES:
                        raise TransportStop("Crossref response exceeds the byte budget")
                    chunks.append(chunk)
                raw = b"".join(chunks)
                response._content = raw
                response._content_consumed = True
                data = response.json()
                items = data.get("message", {}).get("items", [])
                if not isinstance(items, list) or len(items) > MAX_PAPERS or (page_count == 2 and items):
                    raise TransportStop("Crossref paging did not respect the hard article cap")
                if page_count == 1:
                    if not 1 <= len(items) <= MAX_PAPERS:
                        raise TransportStop("Crossref returned no bounded journal articles")
                    dois = [item.get("DOI", "") for item in items]
                    if len(set(dois)) != len(dois) or any(not doi.startswith("10.1371/") for doi in dois):
                        raise TransportStop("Unexpected public journal DOI set")
                    if any(html.unescape((item.get("container-title") or [""])[0]).upper() != JOURNAL or not item.get("author") for item in items):
                        raise TransportStop("Public articles do not match the real collector's journal/author conditions")
                    import psycopg2
                    with psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15) as connection:
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT count(*) FROM public.journals WHERE doi=ANY(%s)", (dois,))
                            if cursor.fetchone()[0]:
                                raise TransportStop("Public DOI already exists; refusing reuse or cleanup ownership")
                    state["owned_dois"] = dois
                    # Durable intent must exist before the unchanged skill INSERT.
                    write_state(state)
                state["pages"][slot] = {"url": response.url, "status": response.status_code, "body": data, "sha256": hashlib.sha256(raw).hexdigest()}
                write_state(state)
                return response
            except (requests.RequestException, ValueError, OSError):
                raise TransportStop("Real Crossref transport failed; no retry is permitted") from None
            finally:
                session.close()
        elif mode == "replay":
            page = state["pages"][slot]
            response = requests.Response()
            response.status_code = page["status"]
            response.url = page["url"]
            response._content = json.dumps(page["body"]).encode()
            response._content_consumed = True
            return response
        raise TransportStop("Unsupported Crossref transport mode")

    requests.get = get


def qualify(target_path: Path) -> dict:
    target = load_private(target_path)
    validate_target(target)
    assert target["project_ref"] == BRANCH_REF
    kb_root, skills_root = qualification_sources()
    state_path = target_path.parent / "crossref-live.private.json"
    receipt_path = target_path.parent / "crossref-live.json"
    if state_path.exists() or receipt_path.exists():
        raise SafeError("Crossref qualification already has a receipt; refusing an unreviewed repeat")
    state = {"project_ref": BRANCH_REF, "run_id": target["run_id"], "public_source": "Crossref", "synthetic_papers": False, "issn": ISSN, "journal": JOURNAL, "owned_issn_id": str(uuid.uuid4()), "owned_dois": [], "requests": [], "pages": {}, "cleanup": False}
    private_json(state_path, state)
    old_environment = dict(os.environ)
    connection = None
    created_issn = False
    failure = None
    result = None
    with tempfile.TemporaryDirectory(prefix="tiangong-public-crossref-") as temporary_name:
        temporary = Path(temporary_name)
        empty_env = temporary / "empty.env"
        empty_env.write_text("")
        copied_skills = temporary / "skills"
        hashes = {}
        for skill, filename in (("fetch-meta-to-kb", "fetch_meta_to_kb.py"), ("fetch-abstract-to-kb", "fetch_abstract_to_kb.py")):
            source = skills_root / skill / "scripts" / filename
            destination = copied_skills / skill / "scripts" / filename
            destination.parent.mkdir(parents=True)
            shutil.copyfile(source, destination)
            hashes[str(source.relative_to(WORKSPACE))] = hashlib.sha256(destination.read_bytes()).hexdigest()
        bootstrap = temporary / "bootstrap"
        bootstrap.mkdir()
        source = Path(__file__).resolve()
        # Import the owned harness as a transport adapter in the original skill subprocess.
        (bootstrap / "sitecustomize.py").write_text(f"import sys\nsys.path.insert(0, {str(source.parent)!r})\nfrom crossref_check import install_transport\ninstall_transport()\n")
        environment = {key: value for key, value in old_environment.items() if key in ("PATH", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT")}
        environment.update({"DATABASE_URL": target["db_url"], "KB_ENV_FILE": str(empty_env), "PYTHON_DOTENV_DISABLED": "1", "KB_SKILLS_DIR": str(copied_skills), "KB_LOG_DIR": str(temporary / "logs"), "KB_ABSTRACT_STATE_PATH": str(temporary / "abstract-state.json"), "PYTHONPATH": str(bootstrap), "CROSSREF_CHECK_STATE": str(state_path), "CROSSREF_CHECK_MODE": "record"})
        try:
            os.environ.clear()
            os.environ.update(environment)
            import psycopg2
            from psycopg2.extensions import parse_dsn
            settings = parse_dsn(target["db_url"])
            for source_key, env_key in {"host": "KB_DB_HOST", "port": "KB_DB_PORT", "dbname": "KB_DB_NAME", "user": "KB_DB_USER", "password": "KB_DB_PASSWORD", "sslmode": "PGSSLMODE"}.items():
                if settings.get(source_key):
                    os.environ[env_key] = settings[source_key]
            os.environ.setdefault("PGSSLMODE", "require")
            connection = psycopg2.connect(target["db_url"], connect_timeout=15)
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='20s'")
                cursor.execute("SELECT count(*) FROM public.journals_issn")
                if cursor.fetchone()[0] != 0:
                    raise SafeError("Cannot bound the unchanged collector with other ISSN rows present")
                cursor.execute("INSERT INTO public.journals_issn(id,journal,issn1) VALUES(%s,%s,%s)", (state["owned_issn_id"], JOURNAL, ISSN))
            connection.commit()
            created_issn = True
            sys.path.insert(0, str(kb_root / "src/file_processing/journals"))
            import collect_metadata
            import kb_abstract_pipeline as pipeline
            for filename in ("collect_metadata.py", "kb_runtime.py", "kb_abstract_pipeline.py"):
                path = kb_root / "src/file_processing/journals" / filename
                hashes[str(path.relative_to(WORKSPACE))] = hashlib.sha256(path.read_bytes()).hexdigest()
            inserted_counts = []
            for mode in ("record", "replay"):
                os.environ["CROSSREF_CHECK_MODE"] = mode
                previous_arguments = sys.argv
                try:
                    sys.argv = ["collect_metadata.py", "--from-date", "2000-01-01", "--command-timeout", "75"]
                    with redirect_stdout(io.StringIO()):
                        status = collect_metadata.main()
                    if status != 0:
                        raise SafeError("The unchanged KB metadata collector failed within its bounded transport")
                finally:
                    sys.argv = previous_arguments
                report = json.loads((temporary / "logs/kb-meta-run-result.json").read_text())
                state = json.loads(state_path.read_text())
                expected = len(state["owned_dois"]) if mode == "record" else 0
                if not report["ok"] or not report["summary"]["completed"] or report["summary"]["total_inserted"] != expected:
                    raise SafeError("Actual collector insertion or DOI deduplication did not match the bounded public sample")
                inserted_counts.append(expected)
            dois = state["owned_dois"]
            with connection.cursor() as cursor:
                cursor.execute("SELECT doi,authors,abstract FROM public.journals WHERE doi=ANY(%s)", (dois,))
                stored = cursor.fetchall()
            connection.rollback()
            if len(stored) != len(dois) or any(not isinstance(authors, list) or not authors for _, authors, _ in stored):
                raise SafeError("Real public metadata did not round-trip its author arrays")

            public_items = state["pages"]["1"]["body"]["message"]["items"]
            has_abstract = next((item for item in public_items if pipeline.clean_abstract(item.get("abstract"))), None)
            backfill = {"upstream_abstract_present": bool(has_abstract), "qualified": False}
            if has_abstract:
                selected_doi = has_abstract["DOI"]
                # A controlled empty-field fixture on our new row exercises backfill.
                with connection.cursor() as cursor:
                    cursor.execute("UPDATE public.journals SET abstract=NULL WHERE doi=%s", (selected_doi,))
                connection.commit()
                readonly = pipeline.database_connection(readonly=True)
                try:
                    candidates = [row for row in pipeline.database_rows(readonly) if row["doi"] == selected_doi]
                finally:
                    readonly.close()
                if len(candidates) != 1:
                    raise SafeError("Actual backfill candidate query did not find our public DOI")
                original_urlopen = pipeline.urlopen
                def bounded_urlopen(request, timeout=None):
                    url = request.full_url if hasattr(request, "full_url") else str(request)
                    if url != "https://api.crossref.org/works/" + quote(selected_doi, safe=""):
                        raise SafeError("Abstract resolver attempted an unapproved provider or DOI")
                    current = json.loads(state_path.read_text())
                    if len(current["requests"]) >= MAX_REQUESTS:
                        raise SafeError("Crossref total request budget exhausted")
                    current["requests"].append({"source": "Crossref public abstract", "doi": selected_doi})
                    private_json(state_path, current)
                    return build_opener(NoRedirect()).open(request, timeout=min(timeout or 15, 15))
                pipeline.urlopen = bounded_urlopen
                try:
                    resolved = pipeline.resolve_one(candidates[0], pipeline.PublicSources(timeout=15))
                finally:
                    pipeline.urlopen = original_urlopen
                if resolved["status"] != "success" or resolved.get("source") != "crossref":
                    raise SafeError("The selected real Crossref abstract could not be resolved")
                outcome = pipeline.write_abstracts([resolved])
                repeated = pipeline.write_abstracts([{**resolved, "abstract": "Must not overwrite existing public abstract"}])
                if not outcome["ok"] or outcome["result"]["updated"] != 1 or not repeated["ok"] or repeated["result"]["updated"] != 0 or repeated["result"]["already_present"] != 1:
                    raise SafeError("Actual guarded abstract update did not preserve an existing value")
                with connection.cursor() as cursor:
                    cursor.execute("SELECT abstract FROM public.journals WHERE doi=%s", (selected_doi,))
                    if cursor.fetchone()[0] != resolved["abstract"]:
                        raise SafeError("Repeated backfill overwrote the public abstract")
                connection.rollback()
                backfill = {"upstream_abstract_present": True, "qualified": True, "controlled_empty_field": True, "updated": 1, "repeat_updated": 0, "existing_value_preserved": True}
            result = {"ok": True, "project_ref": BRANCH_REF, "run_id": target["run_id"], "public_source": "Crossref", "synthetic_papers": False, "public_paper_count": len(dois), "metadata_insert_counts": inserted_counts, "duplicate_run_transport": "Explicit local cache of the first real Crossref HTTP responses", "paging_bound": "Two-row real page then a real rows=0 summary page", "author_arrays_verified": True, "abstract_backfill": backfill, "source_sha256": hashes}
        except Exception as error:
            failure = error
        finally:
            try:
                if connection is not None:
                    connection.rollback()
                    current = json.loads(state_path.read_text())
                    with connection.cursor() as cursor:
                        if current["owned_dois"]:
                            cursor.execute("DELETE FROM public.journals WHERE doi=ANY(%s)", (current["owned_dois"],))
                        if created_issn:
                            cursor.execute("DELETE FROM public.journals_issn WHERE id=%s AND journal=%s AND issn1=%s", (current["owned_issn_id"], JOURNAL, ISSN))
                    connection.commit()
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT count(*) FROM public.journals WHERE doi=ANY(%s)", (current["owned_dois"],))
                        journals_left = cursor.fetchone()[0]
                        cursor.execute("SELECT count(*) FROM public.journals_issn WHERE id=%s", (current["owned_issn_id"],))
                        issn_left = cursor.fetchone()[0]
                    connection.rollback()
                    if journals_left or issn_left:
                        raise SafeError("Owned Crossref fixtures remain after cleanup")
                    current["cleanup"] = True
                    private_json(state_path, current)
                    connection.close()
            finally:
                os.environ.clear()
                os.environ.update(old_environment)
    state = json.loads(state_path.read_text())
    receipt = result or {"ok": False, "project_ref": BRANCH_REF, "run_id": target["run_id"], "error": "Bounded actual Crossref qualification failed; private payload and diagnostics are withheld.", "error_type": type(failure).__name__ if failure else "Unknown"}
    receipt.update({"request_count": len(state["requests"]), "request_limit": MAX_REQUESTS, "paper_limit": MAX_PAPERS, "fixtures_cleaned": state["cleanup"], "completed_at": datetime.now(timezone.utc).isoformat()})
    private_json(receipt_path, receipt)
    if failure:
        raise SafeError(receipt["error"]) from None
    return receipt


def validate_observed_sample(state: dict, target: dict) -> list[str]:
    validate_target(target)
    dois = state.get("owned_dois", [])
    if state.get("project_ref") != target["project_ref"] or state.get("run_id") != target["run_id"] or state.get("cleanup") is not True:
        raise SafeError("Observed public sample does not belong to this cleaned test run")
    if not isinstance(dois, list) or not 1 <= len(dois) <= MAX_PAPERS or len(set(dois)) != len(dois) or any(not isinstance(doi, str) or not doi.startswith("10.1371/") for doi in dois):
        raise SafeError("Observed public DOI set exceeds the authorized sample")
    if not isinstance(state.get("requests"), list) or len(state["requests"]) > MAX_REQUESTS:
        raise SafeError("Invalid Crossref request ledger")
    return dois


def record_abstract_request(state: dict, target: dict, doi: str) -> None:
    dois = validate_observed_sample(state, target)
    if doi not in dois or len(state["requests"]) >= MAX_REQUESTS:
        raise SafeError("Abstract request exceeds the exact observed DOI/request budget")
    state["requests"].append({"source": "Crossref public single-work abstract confirmation", "doi": doi})


def confirm_existing_abstracts(target_path: Path) -> dict:
    """At most two further real GETs for the existing public DOI identities."""
    target = load_private(target_path)
    state_path = target_path.parent / "crossref-live.private.json"
    receipt_path = target_path.parent / "crossref-live.json"
    state = json.loads(state_path.read_text())
    dois = validate_observed_sample(state, target)
    receipt = json.loads(receipt_path.read_text())
    if not receipt.get("ok") or not receipt.get("fixtures_cleaned") or receipt.get("abstract_confirmation"):
        raise SafeError("A cleaned successful public sample is required; confirmation cannot be repeated")
    kb_root, skills_root = qualification_sources()
    old_environment = dict(os.environ)
    connection = None
    inserted = []
    outcome = None
    error = None
    confirmation = []
    with tempfile.TemporaryDirectory(prefix="tiangong-crossref-abstract-confirm-") as directory:
        temporary = Path(directory)
        empty_env = temporary / "empty.env"
        empty_env.write_text("")
        environment = {key: value for key, value in old_environment.items() if key in ("PATH", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT")}
        environment.update({"DATABASE_URL": target["db_url"], "KB_ENV_FILE": str(empty_env), "PYTHON_DOTENV_DISABLED": "1", "KB_SKILLS_DIR": str(skills_root), "KB_LOG_DIR": str(temporary / "logs"), "KB_ABSTRACT_STATE_PATH": str(temporary / "abstract-state.json")})
        try:
            os.environ.clear()
            os.environ.update(environment)
            sys.path.insert(0, str(kb_root / "src/file_processing/journals"))
            import kb_abstract_pipeline as pipeline
            import kb_runtime
            import psycopg2
            from psycopg2.extras import Json
            connection = psycopg2.connect(target["db_url"], connect_timeout=15)
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='20s'")
                cursor.execute("SELECT count(*) FROM public.journals WHERE doi=ANY(%s)", (dois,))
                if cursor.fetchone()[0]:
                    raise SafeError("Observed public DOI appeared after cleanup; refusing to adopt it")
            connection.rollback()
            public_items = state["pages"]["1"]["body"]["message"]["items"]
            metadata_skill = kb_runtime.load_skill("fetch-meta-to-kb", "fetch_meta_to_kb.py")
            frame = metadata_skill.create_dataframe(public_items, JOURNAL)
            original_urlopen = pipeline.urlopen
            selected_doi = ""
            def bounded_urlopen(request, timeout=None):
                url = request.full_url if hasattr(request, "full_url") else str(request)
                if url != "https://api.crossref.org/works/" + quote(selected_doi, safe=""):
                    # Other providers are outside this two-public-paper authorization.
                    raise pipeline.SourceError("provider_not_authorized_in_crossref_only_qualification", stop_source=True)
                current = json.loads(state_path.read_text())
                record_abstract_request(current, target, selected_doi)
                private_json(state_path, current)
                return build_opener(NoRedirect()).open(request, timeout=min(timeout or 15, 15))
            pipeline.urlopen = bounded_urlopen
            try:
                resolved = []
                for doi in dois:
                    selected_doi = doi
                    item = next(item for item in public_items if item["DOI"] == doi)
                    result = pipeline.resolve_one({"doi": doi, "title": (item.get("title") or [""])[0]}, pipeline.PublicSources(timeout=15))
                    confirmation.append({"doi": doi, "crossref_abstract_available": result.get("status") == "success" and result.get("source") == "crossref", "resolver_status": result.get("status")})
                    if result.get("status") == "success" and result.get("source") == "crossref":
                        resolved.append(result)
            finally:
                pipeline.urlopen = original_urlopen
            # Only the exact two previously absent DOI identities are inserted.
            # Empty fields are controlled test fixtures, not fabricated abstracts.
            current = json.loads(state_path.read_text())
            current["confirmation_owned_dois"] = list(dois)
            current["cleanup"] = False
            private_json(state_path, current)
            with connection.cursor() as cursor:
                for _, row in frame.iterrows():
                    if row["doi"] not in dois:
                        raise SafeError("Recorded DataFrame attempted an unobserved DOI")
                    cursor.execute("INSERT INTO public.journals(title,doi,journal,authors,date,abstract) VALUES(%s,%s,%s,%s,%s,NULL)", (row["title"], row["doi"], row["journal"], Json(row["authors"]), row["date"]))
                    inserted.append(row["doi"])
            connection.commit()
            readonly = pipeline.database_connection(readonly=True)
            try:
                candidates = [row for row in pipeline.database_rows(readonly) if row["doi"] in dois]
            finally:
                readonly.close()
            if {row["doi"] for row in candidates} != set(dois):
                raise SafeError("Real candidate query did not return the two controlled empty fields")
            if resolved:
                written = pipeline.write_abstracts(resolved)
                repeated = pipeline.write_abstracts([{**item, "abstract": "Must not overwrite an existing public abstract"} for item in resolved])
                if not written["ok"] or written["result"]["updated"] != len(resolved) or not repeated["ok"] or repeated["result"]["updated"] != 0 or repeated["result"]["already_present"] != len(resolved):
                    raise SafeError("Real guarded public abstract write/repeat contract failed")
                with connection.cursor() as cursor:
                    for item in resolved:
                        cursor.execute("SELECT abstract FROM public.journals WHERE doi=%s", (item["doi"],))
                        if cursor.fetchone()[0] != item["abstract"]:
                            raise SafeError("Guarded repeat changed a real public abstract")
                connection.rollback()
                outcome = {"upstream_abstract_present": True, "qualified": True, "controlled_empty_field": True, "candidate_count": len(candidates), "updated": len(resolved), "repeat_updated": 0, "existing_value_preserved": True}
            else:
                # No-data output comes from real Crossref work responses. Do not
                # claim a positive write or invoke unapproved fallback providers.
                with connection.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM public.journals WHERE doi=ANY(%s) AND abstract IS NOT NULL", (dois,))
                    if cursor.fetchone()[0] != 0:
                        raise SafeError("No-data abstract resolution unexpectedly wrote a value")
                connection.rollback()
                outcome = {"upstream_abstract_present": False, "qualified": False, "no_data_skip_verified": True, "candidate_count": len(candidates), "updated": 0, "reason": "The two real Crossref single-work responses did not contain public abstracts; other provider fallbacks were not authorized"}
        except Exception as failure:
            error = failure
        finally:
            try:
                if connection is not None:
                    connection.rollback()
                    with connection.cursor() as cursor:
                        for doi in inserted:
                            cursor.execute("DELETE FROM public.journals WHERE doi=%s", (doi,))
                    connection.commit()
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT count(*) FROM public.journals WHERE doi=ANY(%s)", (inserted,))
                        if cursor.fetchone()[0] != 0:
                            raise SafeError("Public abstract confirmation left owned rows")
                    connection.rollback()
                    current = json.loads(state_path.read_text())
                    current["cleanup"] = True
                    current["confirmation_owned_dois"] = []
                    private_json(state_path, current)
                    connection.close()
            finally:
                os.environ.clear()
                os.environ.update(old_environment)
    state = json.loads(state_path.read_text())
    receipt["abstract_confirmation"] = confirmation
    if outcome is not None:
        receipt["abstract_backfill"] = outcome
    receipt["request_count"] = len(state["requests"])
    receipt["fixtures_cleaned"] = True
    receipt["abstract_confirmation_ok"] = error is None
    private_json(receipt_path, receipt)
    if error:
        raise SafeError("Crossref confirmation failed; provider/target diagnostics withheld") from None
    return receipt


def validate_positive_state(state: dict, target: dict) -> None:
    validate_target(target)
    if state.get("schema_version") != 1 or state.get("project_ref") != target["project_ref"] or state.get("run_id") != target["run_id"]:
        raise SafeError("Positive sample manifest does not belong to this run")
    rows = state.get("owned_rows", [])
    if not isinstance(rows, list) or len(rows) != 2 or {row.get("doi") for row in rows} != set(POSITIVE_SAMPLE):
        raise SafeError("Positive sample must contain only the two reviewed public DOIs")
    identifiers = []
    for row in rows:
        try:
            identifier = uuid.UUID(row["id"])
        except (KeyError, ValueError, TypeError, AttributeError):
            raise SafeError("Invalid owned positive sample UUID") from None
        if identifier.version != 4 or row.get("owner_run_id") != target["run_id"]:
            raise SafeError("Positive sample UUID intent is not bound to this run")
        identifiers.append(str(identifier))
    if len(set(identifiers)) != 2:
        raise SafeError("Positive sample UUIDs must be distinct")
    requests = state.get("requests", [])
    if not isinstance(requests, list) or len(requests) > POSITIVE_MAX_REQUESTS:
        raise SafeError("Positive sample request ledger exceeds its budget")
    seen = set()
    for request in requests:
        identity = (request.get("kind"), request.get("doi"))
        if identity[0] not in ("metadata", "resolver") or identity[1] not in POSITIVE_SAMPLE or identity in seen:
            raise SafeError("Positive sample request ledger has an unapproved or repeated request")
        seen.add(identity)


def record_positive_request(state: dict, target: dict, kind: str, doi: str) -> None:
    validate_positive_state(state, target)
    if kind not in ("metadata", "resolver") or doi not in POSITIVE_SAMPLE or len(state["requests"]) >= POSITIVE_MAX_REQUESTS:
        raise SafeError("Positive sample exact DOI/request budget exceeded")
    if any(item["kind"] == kind and item["doi"] == doi for item in state["requests"]):
        raise SafeError("Positive sample provider retries are forbidden")
    state["requests"].append({"kind": kind, "doi": doi})


def verified_source_copy(source: Path, destination: Path) -> str:
    original = source.read_bytes()
    if destination.read_bytes() != original:
        raise SafeError("Positive qualification changed the actual consumer source bytes")
    return hashlib.sha256(original).hexdigest()


def cleanup_positive_rows(connection, state: dict, target: dict) -> None:
    validate_positive_state(state, target)
    connection.rollback()
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout='20s'")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        for row in state["owned_rows"]:
            if row.get("insert_requested"):
                cursor.execute("DELETE FROM public.journals WHERE id=%s AND doi=%s", (row["id"], row["doi"]))
                cursor.execute("SELECT count(*) FROM public.journals WHERE id=%s", (row["id"],))
                if cursor.fetchone()[0] != 0:
                    raise SafeError("Positive fixture UUID belongs to a foreign DOI; cleanup refused")
    connection.commit()
    with connection.cursor() as cursor:
        for row in state["owned_rows"]:
            if row.get("insert_requested"):
                cursor.execute("SELECT count(*) FROM public.journals WHERE id=%s AND doi=%s", (row["id"], row["doi"]))
                if cursor.fetchone()[0] != 0:
                    raise SafeError("Owned positive abstract fixture remains after cleanup")
    connection.rollback()
    state["cleanup_verified"] = True


def cleanup_positive_abstracts(target_path: Path) -> dict:
    """Retry only precise owned-row cleanup after an interrupted qualification."""
    target = load_private(target_path)
    state_path = target_path.parent / "crossref-positive-abstract.private.json"
    if state_path.is_symlink() or not state_path.is_file() or state_path.stat().st_mode & 0o077:
        raise SafeError("Positive cleanup requires its private regular 0600 ownership manifest")
    state = json.loads(state_path.read_text())
    validate_positive_state(state, target)
    import psycopg2
    with psycopg2.connect(target["db_url"], connect_timeout=15) as connection:
        cleanup_positive_rows(connection, state, target)
    private_json(state_path, state)
    return {"ok": True, "operation": "cleanup-positive-abstracts", "run_id": target["run_id"], "fixtures_cleaned": state["cleanup_verified"], "provider_requests": 0}


def qualify_positive_abstracts(target_path: Path) -> dict:
    """Fresh public metadata, real empty-field candidates and live Crossref resolution.

    This operation is separate from the historical PLOS sample/receipts. Every
    positive response comes from a new real GET, never recorded-response replay.
    """
    target = load_private(target_path)
    state_path = target_path.parent / "crossref-positive-abstract.private.json"
    receipt_path = target_path.parent / "crossref-positive-abstract.json"
    if state_path.exists() or receipt_path.exists():
        raise SafeError("Positive abstract qualification already has a receipt; refusing reuse")
    kb_root, skills_root = qualification_sources()
    state = {"schema_version": 1, "project_ref": target["project_ref"], "run_id": target["run_id"],
             "owned_rows": [{"doi": doi, "id": str(uuid.uuid4()), "owner_run_id": target["run_id"], "insert_requested": False} for doi in POSITIVE_SAMPLE],
             "requests": [], "responses": [], "cleanup_verified": False, "source_sha256": {}}
    validate_positive_state(state, target)
    private_json(state_path, state)  # Persist all UUID/DOI intent before network or INSERT.
    previous_environment = dict(os.environ)
    connection = None
    failure = None
    result = None
    deadline = time.monotonic() + 180
    with tempfile.TemporaryDirectory(prefix="tiangong-positive-abstract-") as directory:
        temporary = Path(directory)
        empty_env = temporary / "empty.env"
        empty_env.write_text("")
        copied_skills = temporary / "skills"
        for name, filename in (("fetch-meta-to-kb", "fetch_meta_to_kb.py"), ("fetch-abstract-to-kb", "fetch_abstract_to_kb.py")):
            source = skills_root / name / "scripts" / filename
            destination = copied_skills / name / "scripts" / filename
            destination.parent.mkdir(parents=True)
            shutil.copyfile(source, destination)
            state["source_sha256"][str(source.relative_to(WORKSPACE))] = verified_source_copy(source, destination)
        private_json(state_path, state)
        environment = {key: value for key, value in previous_environment.items() if key in ("PATH", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT")}
        environment.update({"DATABASE_URL": target["db_url"], "KB_ENV_FILE": str(empty_env), "PYTHON_DOTENV_DISABLED": "1", "KB_SKILLS_DIR": str(copied_skills), "KB_LOG_DIR": str(temporary / "logs"), "KB_ABSTRACT_STATE_PATH": str(temporary / "abstract-state.json")})
        try:
            os.environ.clear()
            os.environ.update(environment)
            sys.path.insert(0, str(kb_root / "src/file_processing/journals"))
            import kb_abstract_pipeline as pipeline
            import kb_runtime
            import psycopg2
            from psycopg2.extras import Json

            for name in ("kb_abstract_pipeline.py", "kb_runtime.py"):
                source = kb_root / "src/file_processing/journals" / name
                state["source_sha256"][str(source.relative_to(WORKSPACE))] = hashlib.sha256(source.read_bytes()).hexdigest()
            private_json(state_path, state)
            metadata_skill = kb_runtime.load_skill("fetch-meta-to-kb", "fetch_meta_to_kb.py")
            # Frames are built only from fresh metadata, using the original skill.
            def open_live(request, kind, doi, timeout=15):
                expected = "https://api.crossref.org/works/" + quote(doi, safe="")
                actual = request.full_url if hasattr(request, "full_url") else str(request)
                if actual != expected or time.monotonic() >= deadline:
                    raise TransportStop("Positive Crossref source/deadline limit exceeded")
                record_positive_request(state, target, kind, doi)
                private_json(state_path, state)
                return build_opener(NoRedirect()).open(request, timeout=min(timeout, 20, max(1, deadline - time.monotonic())))

            def save_response(raw, kind, doi):
                if len(raw) > MAX_BYTES:
                    raise TransportStop("Positive Crossref response exceeds the byte budget")
                decoded = json.loads(raw)
                if decoded.get("status") != "ok" or decoded.get("message", {}).get("DOI") != doi:
                    raise TransportStop("Positive Crossref response identity differs from the reviewed DOI")
                filename = f"crossref-positive-{kind}-{list(POSITIVE_SAMPLE).index(doi) + 1}.private.json"
                private_json(target_path.parent / filename, decoded)
                state["responses"].append({"kind": kind, "doi": doi, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "private_file": filename})
                private_json(state_path, state)
                return decoded

            from urllib.request import Request
            frames = []
            for doi in POSITIVE_SAMPLE:
                request = Request("https://api.crossref.org/works/" + quote(doi, safe=""), headers={"Accept": "application/json", "User-Agent": "Tiangong-Isolated-Positive-Abstract/1.0"})
                with open_live(request, "metadata", doi, 20) as response:
                    if response.status != 200:
                        raise TransportStop("Crossref metadata did not return HTTP 200")
                    record = save_response(response.read(MAX_BYTES + 1), "metadata", doi)["message"]
                journal = html.unescape((record.get("container-title") or [""])[0])
                if journal != POSITIVE_SAMPLE[doi]["journal"] or pipeline.TERMINAL_TITLE.match((record.get("title") or [""])[0].strip()):
                    raise SafeError("Public paper does not match the reviewed journal/article type")
                frame = metadata_skill.create_dataframe([record], journal)
                if len(frame) != 1 or frame.iloc[0]["doi"] != doi or not frame.iloc[0]["authors"]:
                    raise SafeError("Actual metadata skill rejected the reviewed public paper")
                frames.append(frame.iloc[0])
            connection = psycopg2.connect(target["db_url"], connect_timeout=15)
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='20s'")
                cursor.execute("SET lock_timeout='5s'")
                cursor.execute("SELECT count(*) FROM public.journals WHERE doi=ANY(%s) OR id=ANY(%s::uuid[])", (list(POSITIVE_SAMPLE), [row["id"] for row in state["owned_rows"]]))
                if cursor.fetchone()[0] != 0:
                    raise SafeError("Positive DOI/UUID already exists; refusing to adopt another row")
            connection.rollback()
            for row in state["owned_rows"]:
                row["insert_requested"] = True
            private_json(state_path, state)
            with connection.cursor() as cursor:
                for owned_row, metadata_row in zip(state["owned_rows"], frames):
                    cursor.execute("INSERT INTO public.journals(id,title,doi,journal,authors,date,abstract) VALUES(%s,%s,%s,%s,%s,%s,NULL)", (owned_row["id"], metadata_row["title"], metadata_row["doi"], metadata_row["journal"], Json(metadata_row["authors"]), metadata_row["date"]))
            connection.commit()
            readonly = pipeline.database_connection(readonly=True)
            try:
                candidates = [row for row in pipeline.database_rows(readonly) if row["doi"] in POSITIVE_SAMPLE]
            finally:
                readonly.close()
            if len(candidates) != 2 or {row["doi"] for row in candidates} != set(POSITIVE_SAMPLE):
                raise SafeError("Actual KB candidate query did not find exactly the two empty fields")

            # The actual resolver consumes the real live HTTP stream. This wrapper
            # only enforces exact URLs/budgets and records bytes; no cached body,
            # synthesized provider response or fallback provider is supplied.
            class RecordedResponse:
                def __init__(self, response, doi):
                    self.response, self.doi = response, doi
                def __enter__(self):
                    return self
                def __exit__(self, *_args):
                    self.response.close()
                def read(self, size):
                    raw = self.response.read(min(size, MAX_BYTES + 1))
                    save_response(raw, "resolver", self.doi)
                    return raw
            urls = {"https://api.crossref.org/works/" + quote(doi, safe=""): doi for doi in POSITIVE_SAMPLE}
            def resolver_urlopen(request, timeout=None):
                actual = request.full_url if hasattr(request, "full_url") else str(request)
                doi = urls.get(actual)
                if doi is None:
                    raise TransportStop("Positive qualification forbids other providers or DOI identities")
                response = open_live(request, "resolver", doi, timeout or 15)
                if response.status != 200:
                    response.close()
                    raise TransportStop("Positive resolver source did not return HTTP 200")
                return RecordedResponse(response, doi)
            original_urlopen = pipeline.urlopen
            pipeline.urlopen = resolver_urlopen
            try:
                resolved = [pipeline.resolve_one(row, pipeline.PublicSources(timeout=15)) for row in candidates]
            finally:
                pipeline.urlopen = original_urlopen
            for item in resolved:
                expected = POSITIVE_SAMPLE[item["doi"]]
                if item["status"] != "success" or item.get("source") != "crossref" or len(item.get("abstract", "")) != expected["length"] or hashlib.sha256(item.get("abstract", "").encode()).hexdigest() != expected["sha256"]:
                    raise SafeError("Live positive abstract does not match its reviewed content fingerprint")
            written = pipeline.write_abstracts(resolved)
            # Use the same real public abstract again, not fabricated replacement.
            repeated = pipeline.write_abstracts(resolved)
            if not written["ok"] or written["result"]["updated"] != 2 or not repeated["ok"] or repeated["result"]["updated"] != 0 or repeated["result"]["already_present"] != 2:
                raise SafeError("Actual guarded abstract update/repeat did not preserve existing fields")
            with connection.cursor() as cursor:
                observed = []
                for owned_row in state["owned_rows"]:
                    cursor.execute("SELECT abstract FROM public.journals WHERE id=%s AND doi=%s", (owned_row["id"], owned_row["doi"]))
                    value = cursor.fetchone()
                    digest = hashlib.sha256(value[0].encode()).hexdigest() if value and isinstance(value[0], str) else None
                    if digest != POSITIVE_SAMPLE[owned_row["doi"]]["sha256"]:
                        raise SafeError("Positive abstract readback changed a nonempty field")
                    observed.append({"doi": owned_row["doi"], "length": len(value[0]), "sha256": digest})
            connection.rollback()
            readonly = pipeline.database_connection(readonly=True)
            try:
                remaining = [row for row in pipeline.database_rows(readonly) if row["doi"] in POSITIVE_SAMPLE]
            finally:
                readonly.close()
            if remaining:
                raise SafeError("Positive papers remain eligible after actual abstract writes")
            result = {"ok": True, "project_ref": target["project_ref"], "run_id": target["run_id"], "public_source": "Crossref", "public_paper_count": 2, "metadata_source": "Fresh live single-work responses through original metadata skill", "candidate_count_before": 2, "candidate_count_after": 0, "abstracts": observed, "abstract_backfill": {"qualified": True, "updated": 2, "repeat_updated": 0, "already_present": 2, "existing_values_preserved": True}, "source_sha256": state["source_sha256"], "mock_or_cached_positive_responses": False}
        except (Exception, TransportStop) as error:
            failure = error
        finally:
            try:
                if connection is not None:
                    try:
                        cleanup_positive_rows(connection, state, target)
                        private_json(state_path, state)
                    finally:
                        connection.close()
                elif not any(row["insert_requested"] for row in state["owned_rows"]):
                    state["cleanup_verified"] = True  # No DB INSERT was attempted.
                    private_json(state_path, state)
            except Exception as cleanup_error:
                failure = cleanup_error
                state["cleanup_verified"] = False
                private_json(state_path, state)
            finally:
                os.environ.clear()
                os.environ.update(previous_environment)
    receipt = result if result is not None and failure is None else {"ok": False, "project_ref": target["project_ref"], "run_id": target["run_id"], "public_paper_count": 2, "error_type": type(failure).__name__ if failure else "Unknown", "error": "Positive public abstract qualification failed; provider and target details withheld."}
    receipt.update({"request_count": len(state["requests"]), "metadata_requests": sum(item["kind"] == "metadata" for item in state["requests"]), "resolver_requests": sum(item["kind"] == "resolver" for item in state["requests"]), "request_limit": POSITIVE_MAX_REQUESTS, "paper_limit": 2, "fixtures_cleaned": state["cleanup_verified"], "completed_at": datetime.now(timezone.utc).isoformat()})
    private_json(receipt_path, receipt)
    if failure:
        raise SafeError(receipt["error"]) from None
    return receipt


def self_test() -> None:
    """Pure budget and hard-target checks; never open a network or DB socket."""
    import unittest
    class SourceTests(unittest.TestCase):
        def test_canonical_legacy_mixed_and_dual_checkouts(self):
            for names, expected in (
                (("knowledge-base", "agent-skills"), ("knowledge-base", "agent-skills")),
                (("TianGong-AI-KB", "skills"), ("TianGong-AI-KB", "skills")),
                (("knowledge-base", "skills"), ("knowledge-base", "skills")),
                (("TianGong-AI-KB", "agent-skills"), ("TianGong-AI-KB", "agent-skills")),
                (("knowledge-base", "agent-skills", "TianGong-AI-KB", "skills"), ("knowledge-base", "agent-skills")),
            ):
                with self.subTest(names=names), tempfile.TemporaryDirectory() as directory:
                    workspace = Path(directory)
                    for name in names:
                        (workspace / name).mkdir()
                    self.assertEqual(qualification_sources(workspace), tuple(workspace / name for name in expected))
        def test_files_are_not_source_checkouts(self):
            with tempfile.TemporaryDirectory() as directory:
                workspace = Path(directory)
                for name in ("knowledge-base", "agent-skills"):
                    (workspace / name).write_text("not a checkout")
                for name in ("TianGong-AI-KB", "skills"):
                    (workspace / name).mkdir()
                self.assertEqual(qualification_sources(workspace), (workspace / "TianGong-AI-KB", workspace / "skills"))
        def test_missing_sources_are_refused(self):
            for names in ((), ("knowledge-base",), ("agent-skills",)):
                with self.subTest(names=names), tempfile.TemporaryDirectory() as directory:
                    workspace = Path(directory)
                    for name in names:
                        (workspace / name).mkdir()
                    with self.assertRaises(SafeError):
                        qualification_sources(workspace)
    target = {"schema_version": 1, "project_ref": BRANCH_REF, "parent_project_ref": "qyyqlnwqwgvzxnccnbgm", "with_data": False, "branch_id": str(uuid.uuid4()), "run_id": "a" * 16, "prefix": "live72-" + "a" * 16 + "-", "api_url": f"https://{BRANCH_REF}.supabase.co", "db_url": f"postgresql://postgres:synthetic@db.{BRANCH_REF}.supabase.co/postgres", "anon_key": "synthetic", "publishable_key": "synthetic", "service_role_key": "synthetic", "run_token": "synthetic"}
    class BudgetTests(unittest.TestCase):
        def state(self):
            return {"project_ref": BRANCH_REF, "run_id": target["run_id"], "cleanup": True, "owned_dois": ["10.1371/test-1", "10.1371/test-2"], "requests": []}
        def test_production_is_refused(self):
            with self.assertRaises(SafeError):
                validate_observed_sample(self.state(), {**target, "project_ref": "qyyqlnwqwgvzxnccnbgm"})
        def test_unobserved_doi_is_refused(self):
            with self.assertRaises(SafeError):
                record_abstract_request(self.state(), target, "10.1371/test-3")
        def test_request_limit_is_exact(self):
            state = self.state()
            for _ in range(MAX_REQUESTS):
                record_abstract_request(state, target, "10.1371/test-1")
            with self.assertRaises(SafeError):
                record_abstract_request(state, target, "10.1371/test-1")
        def test_three_papers_are_refused(self):
            state = self.state()
            state["owned_dois"].append("10.1371/test-3")
            with self.assertRaises(SafeError):
                validate_observed_sample(state, target)
    class PositiveTests(unittest.TestCase):
        def state(self):
            return {"schema_version": 1, "project_ref": BRANCH_REF, "run_id": target["run_id"], "owned_rows": [{"doi": doi, "id": str(uuid.uuid4()), "owner_run_id": target["run_id"], "insert_requested": False} for doi in POSITIVE_SAMPLE], "requests": []}
        def test_exact_reviewed_two_doi_sample(self):
            validate_positive_state(self.state(), target)
            state = self.state()
            state["owned_rows"][0]["doi"] = "10.99999/unreviewed"
            with self.assertRaises(SafeError):
                validate_positive_state(state, target)
        def test_positive_production_and_wrong_run_are_refused(self):
            with self.assertRaises(SafeError):
                validate_positive_state(self.state(), {**target, "project_ref": "qyyqlnwqwgvzxnccnbgm"})
            state = self.state()
            state["owned_rows"][0]["owner_run_id"] = "b" * 16
            with self.assertRaises(SafeError):
                validate_positive_state(state, target)
        def test_duplicate_uuid_and_third_paper_are_refused(self):
            state = self.state()
            state["owned_rows"][1]["id"] = state["owned_rows"][0]["id"]
            with self.assertRaises(SafeError):
                validate_positive_state(state, target)
            state = self.state()
            state["owned_rows"].append({"doi": "10.99999/third"})
            with self.assertRaises(SafeError):
                validate_positive_state(state, target)
        def test_two_fresh_metadata_and_two_resolver_calls_no_retry(self):
            state = self.state()
            for kind in ("metadata", "resolver"):
                for doi in POSITIVE_SAMPLE:
                    record_positive_request(state, target, kind, doi)
            self.assertEqual(len(state["requests"]), 4)
            with self.assertRaises(SafeError):
                record_positive_request(state, target, "resolver", next(iter(POSITIVE_SAMPLE)))
            with self.assertRaises(SafeError):
                record_positive_request(self.state(), target, "resolver", "10.99999/unreviewed")
        def test_repeat_doi_kind_and_unknown_provider_are_refused(self):
            state = self.state()
            doi = next(iter(POSITIVE_SAMPLE))
            record_positive_request(state, target, "resolver", doi)
            with self.assertRaises(SafeError):
                record_positive_request(state, target, "resolver", doi)
            with self.assertRaises(SafeError):
                record_positive_request(self.state(), target, "openalex", doi)
        def test_actual_source_copy_byte_identity(self):
            with tempfile.TemporaryDirectory() as directory:
                source, copied = Path(directory) / "source.py", Path(directory) / "copied.py"
                source.write_bytes(b"# Source identity fixture\r\n")
                shutil.copyfile(source, copied)
                self.assertEqual(verified_source_copy(source, copied), hashlib.sha256(source.read_bytes()).hexdigest())
                copied.write_bytes(b"# Mutated source\n")
                with self.assertRaises(SafeError):
                    verified_source_copy(source, copied)
        def test_cleanup_is_exact_uuid_and_doi_not_broad_doi_only(self):
            class Cursor:
                def __init__(self):
                    self.statements = []
                def __enter__(self):
                    return self
                def __exit__(self, *_args):
                    return None
                def execute(self, sql, parameters=None):
                    self.statements.append((sql, parameters))
                def fetchone(self):
                    return (0,)
            class Connection:
                def __init__(self):
                    self.recording_cursor = Cursor()
                def cursor(self):
                    return self.recording_cursor
                def rollback(self):
                    return None
                def commit(self):
                    return None
            state = self.state()
            for row in state["owned_rows"]:
                row["insert_requested"] = True
            connection = Connection()
            cleanup_positive_rows(connection, state, target)
            deletes = [(statement, parameters) for statement, parameters in connection.recording_cursor.statements if statement.startswith("DELETE")]
            self.assertEqual(len(deletes), 2)
            self.assertTrue(all(statement.endswith("WHERE id=%s AND doi=%s") for statement, _ in deletes))
            self.assertEqual([parameters for _, parameters in deletes], [(row["id"], row["doi"]) for row in state["owned_rows"]])
            self.assertTrue(state["cleanup_verified"])
        def test_planned_but_unattempted_rows_are_not_deleted(self):
            class Cursor:
                def __enter__(self):
                    return self
                def __exit__(self, *_args):
                    return None
                def execute(self, statement, parameters=None):
                    if statement.startswith(("DELETE", "SELECT")):
                        raise AssertionError("Unattempted UUID intent was queried or deleted")
            class Connection:
                def cursor(self):
                    return Cursor()
                def rollback(self):
                    return None
                def commit(self):
                    return None
            state = self.state()
            cleanup_positive_rows(Connection(), state, target)
            self.assertTrue(state["cleanup_verified"])
    suite = unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromTestCase(SourceTests), unittest.defaultTestLoader.loadTestsFromTestCase(BudgetTests), unittest.defaultTestLoader.loadTestsFromTestCase(PositiveTests)])
    results = unittest.TextTestRunner().run(suite)
    if not results.wasSuccessful():
        raise SafeError("Crossref offline safety checks failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path)
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--confirm-existing-abstracts", action="store_true")
    operation.add_argument("--positive-abstract-sample", action="store_true", help="Qualify the two reviewed abstract-bearing public papers using four fresh Crossref GETs.")
    operation.add_argument("--cleanup-positive-abstracts", action="store_true", help="Retry exact UUID/DOI cleanup from the private manifest without provider calls.")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if args.target is None:
        parser.error("--target is required for live qualification")
    try:
        if args.cleanup_positive_abstracts:
            print(json.dumps(cleanup_positive_abstracts(args.target)))
            return
        result = qualify_positive_abstracts(args.target) if args.positive_abstract_sample else confirm_existing_abstracts(args.target) if args.confirm_existing_abstracts else qualify(args.target)
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "error": str(error) if isinstance(error, SafeError) else "Crossref qualification failed; credentials and provider diagnostics withheld."}))
        raise SystemExit(1)
    summary = {"ok": result["ok"], "public_paper_count": result["public_paper_count"], "request_count": result["request_count"], "abstract_backfill_qualified": result["abstract_backfill"]["qualified"], "fixtures_cleaned": result["fixtures_cleaned"], "receipt": str(args.target.parent / ("crossref-positive-abstract.json" if args.positive_abstract_sample else "crossref-live.json"))}
    if args.positive_abstract_sample:
        summary.update({"metadata_requests": result["metadata_requests"], "resolver_requests": result["resolver_requests"], "candidate_count_before": result["candidate_count_before"], "candidate_count_after": result["candidate_count_after"], "updated": result["abstract_backfill"]["updated"], "repeat_updated": result["abstract_backfill"]["repeat_updated"]})
    else:
        summary["metadata_insert_counts"] = result["metadata_insert_counts"]
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
