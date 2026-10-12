"""Offline counterexamples for branch binding, private files and request budgets."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import fixtures
import check


def target():
    ref=fixtures.BRANCH_REF
    return dict(schema_version=1,project_ref=ref,parent_project_ref=fixtures.PRODUCTION_REF,with_data=False,
                branch_id="78590073-6ece-4d1e-8e5b-008b225c850e",run_id="0123456789abcdef",prefix="live72-0123456789abcdef-",
                api_url=f"https://{ref}.supabase.co",db_url=f"postgresql://postgres.{ref}:synthetic@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
                anon_key="synthetic",publishable_key="synthetic",service_role_key="synthetic",run_token="synthetic")


class SafetyTests(unittest.TestCase):
    def test_fixed_branch_and_no_data_required(self):
        fixtures.validate_target(target())
        for field,value in [("project_ref",fixtures.PRODUCTION_REF),("with_data",True),("api_url","https://example.invalid"),("parent_project_ref","another-parent")]:
            changed=target();changed[field]=value
            with self.subTest(field=field),self.assertRaises(fixtures.SafeError): fixtures.validate_target(changed)

    def test_libpq_override_parameters_rejected_even_when_blank(self):
        for key in ("host","hostaddr","user","password","dbname","service","options"):
            for value in ("", "other"):
                changed=target();changed["db_url"]+="?"+key+"="+value
                with self.subTest(key=key,value=value),self.assertRaises(fixtures.SafeError):fixtures.validate_target(changed)

    def test_url_userinfo_redirect_and_fake_domain_rejected(self):
        for url in [f"https://user:pass@{fixtures.BRANCH_REF}.supabase.co",f"https://{fixtures.BRANCH_REF}.supabase.co.example.invalid",f"https://{fixtures.BRANCH_REF}.supabase.co/?host=production"]:
            changed=target();changed["api_url"]=url
            with self.subTest(url=url),self.assertRaises(fixtures.SafeError):fixtures.validate_target(changed)
        with self.assertRaises(fixtures.SafeError):fixtures.NoRedirect().redirect_request(None,None,None,None,None,None)

    def test_private_file_permission_and_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory).resolve()/"target.json";fixtures.private_json(path,target())
            self.assertEqual(fixtures.load_private(path)["project_ref"],fixtures.BRANCH_REF)
            path.chmod(0o644)
            with self.assertRaises(fixtures.SafeError):fixtures.load_private(path)
            path.chmod(0o600);link=path.parent/"link.json";link.symlink_to(path)
            with self.assertRaises(fixtures.SafeError):fixtures.load_private(link)

    def test_budget_rejects_before_network(self):
        budget=check.RequestBudget(target(),Path("/unused"),request_limit=1)
        budget.calls=[{}]
        with patch.object(check,"build_opener",side_effect=AssertionError("network must not run")):
            with self.assertRaises(fixtures.SafeError):budget.request("edu_search",{},dict(email="synthetic@example.invalid",password="synthetic"))
            budget.calls=[];budget.deadline=0
            with self.assertRaises(fixtures.SafeError):budget.request("edu_search",{},dict(email="synthetic@example.invalid",password="synthetic"))

    def test_phase_allowlist(self):
        self.assertEqual(check.phase_sources("edu"),("edu",))
        self.assertEqual(len(check.phase_sources()),4)
        with self.assertRaises(fixtures.SafeError):check.phase_sources("production")

if __name__=="__main__":unittest.main()
