#!/usr/bin/env python3
"""Private synthetic fixtures bound to the explicitly selected empty branch.

No environment file is read. UUID intent is written before Auth/SQL actions.
Cleanup addresses exact owned IDs only and never truncates a table.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import uuid

BRANCH_REF = "qujtuhjbamgzzkixrlay"
PRODUCTION_REF = "qyyqlnwqwgvzxnccnbgm"
REPO = Path(__file__).resolve().parents[2]


class SafeError(RuntimeError):
    pass


def validate_target(target: dict) -> None:
    if target.get("schema_version") != 1 or target.get("project_ref") != BRANCH_REF or target.get("parent_project_ref") != PRODUCTION_REF or target.get("with_data") is not False:
        raise SafeError("Only the fixed data-free preview branch is allowed")
    try:
        uuid.UUID(target["branch_id"])
    except (KeyError, ValueError, TypeError, AttributeError):
        raise SafeError("Missing branch management identity") from None
    run = target.get("run_id", "")
    if not isinstance(run, str) or not re.fullmatch(r"[0-9a-f]{16}", run) or target.get("prefix") != f"live72-{run}-":
        raise SafeError("Invalid synthetic run identity")
    for key in ("api_url", "db_url", "anon_key", "publishable_key", "service_role_key", "run_token"):
        if not isinstance(target.get(key), str) or not target[key]:
            raise SafeError("Missing private target field: " + key)
    try:
        api = urlsplit(target["api_url"])
        db = urlsplit(target["db_url"])
        if api.scheme != "https" or api.hostname != f"{BRANCH_REF}.supabase.co" or api.username or api.password or api.port not in (None, 443) or api.path not in ("", "/") or api.query or api.fragment:
            raise SafeError("API target must exactly match the preview branch")
        if db.scheme not in ("postgres", "postgresql") or db.fragment or db.path != "/postgres":
            raise SafeError("Invalid preview DB URL")
        if PRODUCTION_REF in (db.hostname or "") or PRODUCTION_REF in (db.username or ""):
            raise SafeError("Production DB targets are forbidden")
        if not (db.hostname == f"db.{BRANCH_REF}.supabase.co" and db.username == "postgres") and not (re.fullmatch(r"[a-z0-9-]+[.]pooler[.]supabase[.]com", db.hostname or "") and db.username == f"postgres.{BRANCH_REF}"):
            raise SafeError("DB host and user must bind to the preview branch")
        # libpq accepts target-changing URL parameters; never allow them.
        if any(key not in ("sslmode", "connect_timeout", "application_name") for key in parse_qs(db.query, keep_blank_values=True)):
            raise SafeError("DB URL target override parameters are forbidden")
    except ValueError:
        raise SafeError("Malformed private target URL") from None


def load_private(path: Path) -> dict:
    path = path.absolute()
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077 or path.resolve().is_relative_to(REPO):
        raise SafeError("Target must be a regular 0600 file outside Git")
    if any(parent.is_symlink() for parent in path.parents):
        raise SafeError("Private target path must not contain symlinks")
    try:
        target = json.loads(path.read_text())
    except (ValueError, OSError):
        raise SafeError("Private target JSON cannot be loaded") from None
    if not isinstance(target, dict):
        raise SafeError("Private target must be an object")
    validate_target(target)
    return target


def private_json(path: Path, value: dict) -> None:
    if path.is_symlink() or path.parent.resolve().is_relative_to(REPO):
        raise SafeError("Private receipts must remain outside Git")
    descriptor, temporary = tempfile.mkstemp(prefix=".live-edge-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise SafeError("Credential-bearing redirects are forbidden")


def auth_request(target: dict, method: str, path: str, body=None, *, allow_missing=False):
    validate_target(target)
    if not re.fullmatch(r"admin/users(?:/[0-9a-f-]{36})?", path):
        raise SafeError("Auth operation is outside the fixture allowlist")
    request = Request(target["api_url"].rstrip("/") + "/auth/v1/" + path, method=method,
                      headers={"apikey": target["service_role_key"], "Authorization": "Bearer " + target["service_role_key"], "Content-Type": "application/json"},
                      data=json.dumps(body).encode() if body is not None else None)
    try:
        with build_opener(NoRedirect()).open(request, timeout=25) as response:
            raw = response.read(1_000_001)
            if len(raw) > 1_000_000:
                raise SafeError("Auth response exceeds the fixture budget")
            return json.loads(raw) if raw else {}
    except HTTPError as error:
        if allow_missing and method in ("GET", "DELETE") and error.code == 404:
            return {"already_absent": True}
        raise SafeError(f"Synthetic Auth operation failed HTTP {error.code}; body withheld") from None
    except (URLError, TimeoutError, ValueError):
        raise SafeError("Synthetic Auth operation failed; diagnostics withheld") from None


def validate_manifest(target: dict) -> None:
    validate_target(target)
    f = target.get("fixtures")
    if not isinstance(f, dict) or f.get("marker") != "synthetic" + target["run_id"] or f.get("journal_doi") != "10.99999/" + target["run_id"] + "-sci":
        raise SafeError("Fixture manifest does not bind to this run")
    identifiers = f.get("resource_ids")
    if not isinstance(identifiers, dict) or set(identifiers) != {"education", "esg", "standards"}:
        raise SafeError("Fixture resource identity set is incomplete")
    for value in [*identifiers.values(), f.get("journal_id")]:
        try:
            uuid.UUID(value)
        except (ValueError, TypeError, AttributeError):
            raise SafeError("Invalid fixture UUID") from None
    for name in ("user", "outsider"):
        user = target.get(name, {})
        if user.get("email") != f"{name}-{target['run_id']}@example.invalid":
            raise SafeError("Fixture user is outside this run")
        if not isinstance(user.get("password"), str) or not user["password"]:
            raise SafeError("Fixture password is missing")
        try:
            uuid.UUID(user["id"])
        except (KeyError, ValueError, TypeError, AttributeError):
            raise SafeError("Invalid fixture Auth UUID") from None


def setup(path: Path) -> dict:
    target = load_private(path)
    if "fixtures" in target:
        raise SafeError("A fixture manifest already exists; clean before starting another run")
    import psycopg2
    from psycopg2.extras import Json

    run = target["run_id"]
    marker = "synthetic" + run
    target["fixtures"] = {"marker": marker, "resource_ids": {name: str(uuid.uuid4()) for name in ("education", "esg", "standards")},
                          "edu_course": marker + "course", "edu_name": marker, "journal_id": str(uuid.uuid4()),
                          "journal_doi": "10.99999/" + run + "-sci", "journal_title": marker + "journal", "journal_authors": ["Synthetic Author One", "Synthetic Author Two"]}
    target["fixture_state"] = "planned"
    for name in ("user", "outsider"):
        target[name] = {"id": str(uuid.uuid4()), "email": f"{name}-{run}@example.invalid", "password": secrets.token_urlsafe(30), "state": "planned"}
    validate_manifest(target)
    private_json(path, target)  # Durable intent before any Auth or database mutation.
    for name in ("user", "outsider"):
        user = target[name]
        previous = auth_request(target, "GET", "admin/users/" + user["id"], allow_missing=True)
        if not previous.get("already_absent"):
            raise SafeError("Allocated Auth UUID already exists; refusing reuse")
        user["state"] = "create_requested"
        private_json(path, target)
        result = auth_request(target, "POST", "admin/users", {"id": user["id"], "email": user["email"], "password": user["password"], "email_confirm": True})
        if result.get("id") != user["id"] or result.get("email", "").lower() != user["email"]:
            raise SafeError("Created Auth identity does not match durable fixture intent")
        user["state"] = "created"
        private_json(path, target)
    f = target["fixtures"]
    ids = f["resource_ids"]
    with psycopg2.connect(target["db_url"], connect_timeout=15) as connection:
        with connection.cursor() as cursor:
            cursor.execute("set local statement_timeout='20s'")
            cursor.execute("set local lock_timeout='5s'")
            cursor.execute("INSERT INTO public.edu_meta(id,name,course,type,language,file_type,chapter_number,description) VALUES(%s,%s,%s,'lecture','eng','pdf',1,%s)", (ids["education"], f["edu_name"], f["edu_course"], marker))
            # Null country avoids inventing or sharing a country-code fixture.
            cursor.execute("INSERT INTO public.esg_meta(id,report_title,company_name,company_name_en,country,language,category_new,publication_date) VALUES(%s,%s,%s,%s,NULL,'eng',%s,'2026-01-01')", (ids["esg"], marker, marker + "company", marker + "company", Json([marker])))
            cursor.execute("INSERT INTO public.standards(id,title,standard_number,issuing_organization,language,effective_date) VALUES(%s,%s,%s,%s,'eng','2026-01-01')", (ids["standards"], marker, marker + "standard", Json([marker + "issuer"])))
            cursor.execute("INSERT INTO public.journals(id,doi,title,journal,authors,date,abstract) VALUES(%s,%s,%s,%s,%s,'2026-01-01',%s)", (f["journal_id"], f["journal_doi"], f["journal_title"], marker + "journal", Json(f["journal_authors"]), "Synthetic existing abstract " + run))
    target["fixture_state"] = "ready"
    private_json(path, target)
    return {"operation": "setup", "ok": True, "run_id": run, "synthetic_users": 2, "synthetic_tables": 4}


def cleanup(path: Path) -> dict:
    target = load_private(path)
    if "fixtures" not in target:
        return {"operation": "cleanup", "ok": True, "already_absent": True}
    validate_manifest(target)
    import psycopg2
    from psycopg2 import sql

    f = target["fixtures"]
    # Revoke refresh sessions belonging only to the planned synthetic identities
    # before admin deletion. No JWT/session or production row is exported.
    with psycopg2.connect(target["db_url"], connect_timeout=15) as connection:
        with connection.cursor() as cursor:
            cursor.execute("set local statement_timeout='20s'")
            cursor.execute("set local lock_timeout='5s'")
            for table, identifier in (("edu_meta", f["resource_ids"]["education"]), ("esg_meta", f["resource_ids"]["esg"]), ("standards", f["resource_ids"]["standards"]), ("journals", f["journal_id"])):
                # Only delete exact IDs AND their run-specific identifying field.
                field = {"edu_meta": "name", "esg_meta": "report_title", "standards": "title", "journals": "doi"}[table]
                expected = f["journal_doi"] if table == "journals" else f["marker"]
                cursor.execute(sql.SQL("DELETE FROM public.{} WHERE id=%s AND {}=%s").format(sql.Identifier(table), sql.Identifier(field)), (identifier, expected))
                cursor.execute(sql.SQL("SELECT count(*) FROM public.{} WHERE id=%s").format(sql.Identifier(table)), (identifier,))
                if cursor.fetchone()[0] != 0:
                    raise SafeError("Fixture ID remains; refusing to delete a foreign row")
            for name in ("user", "outsider"):
                user = target[name]
                if user["state"] not in ("create_requested", "created", "deleted"):
                    continue
                current = auth_request(target, "GET", "admin/users/" + user["id"], allow_missing=True)
                if not current.get("already_absent") and current.get("email", "").lower() != user["email"]:
                    raise SafeError("Fixture Auth ID belongs to another email; cleanup refused")
                if not current.get("already_absent"):
                    cursor.execute("DELETE FROM auth.sessions WHERE user_id=%s", (user["id"],))
    for name in ("user", "outsider"):
        user = target[name]
        if user["state"] in ("create_requested", "created", "deleted"):
            auth_request(target, "DELETE", "admin/users/" + user["id"], allow_missing=True)
            if not auth_request(target, "GET", "admin/users/" + user["id"], allow_missing=True).get("already_absent"):
                raise SafeError("Fixture Auth user still exists after deletion")
            user["state"] = "deleted"
            private_json(path, target)
    target["fixture_state"] = "cleaned"
    private_json(path, target)
    return {"operation": "cleanup", "ok": True, "run_id": target["run_id"], "own_rows_absent": True, "own_auth_users_absent": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("setup", "cleanup"))
    parser.add_argument("--target", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        print(json.dumps((setup if arguments.operation == "setup" else cleanup)(arguments.target)))
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__, "error": "Synthetic fixture operation failed; credentials and response bodies withheld."}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
