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
        kb_root = WORKSPACE / "TianGong-AI-KB"
        skills_root = WORKSPACE / "skills"
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
        environment.update({"DATABASE_URL": target["db_url"], "KB_ENV_FILE": str(empty_env), "PYTHON_DOTENV_DISABLED": "1", "KB_SKILLS_DIR": str(WORKSPACE / "skills"), "KB_LOG_DIR": str(temporary / "logs"), "KB_ABSTRACT_STATE_PATH": str(temporary / "abstract-state.json")})
        try:
            os.environ.clear()
            os.environ.update(environment)
            sys.path.insert(0, str(WORKSPACE / "TianGong-AI-KB/src/file_processing/journals"))
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


def self_test() -> None:
    """Pure budget and hard-target checks; never open a network or DB socket."""
    import unittest
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
    results = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(BudgetTests))
    if not results.wasSuccessful():
        raise SafeError("Crossref offline safety checks failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path)
    parser.add_argument("--confirm-existing-abstracts", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if args.target is None:
        parser.error("--target is required for live qualification")
    try:
        result = confirm_existing_abstracts(args.target) if args.confirm_existing_abstracts else qualify(args.target)
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "error": str(error) if isinstance(error, SafeError) else "Crossref qualification failed; credentials and provider diagnostics withheld."}))
        raise SystemExit(1)
    print(json.dumps({"ok": result["ok"], "public_paper_count": result["public_paper_count"], "metadata_insert_counts": result["metadata_insert_counts"], "request_count": result["request_count"], "abstract_backfill_qualified": result["abstract_backfill"]["qualified"], "fixtures_cleaned": result["fixtures_cleaned"], "receipt": str(args.target.parent / "crossref-live.json")}))


if __name__ == "__main__":
    main()
