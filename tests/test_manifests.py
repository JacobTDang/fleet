"""The deploy manifests are only exercised on deploy day, so these guard the
mistakes that would otherwise surface there: a manifest nothing applies, a real
secrets file committed, or an example file shipping a live token."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
K8S = ROOT / "deploy" / "k8s"
SECRET_KEYS = ("NTFY_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "FLEET_FALLBACK_LLM_KEY")


def test_every_manifest_is_listed_in_kustomization():
    kustomization = (K8S / "kustomization.yaml").read_text()
    # applied by hand (secrets) or is the list itself
    excluded = {"kustomization.yaml", "secrets.example.yaml", "secrets.yaml"}
    missing = [p.name for p in sorted(K8S.glob("*.yaml"))
               if p.name not in excluded and p.name not in kustomization]
    assert missing == [], f"manifests never applied — not listed in kustomization.yaml: {missing}"


def test_no_real_secrets_file_is_committed():
    assert not (K8S / "secrets.yaml").exists(), \
        "deploy/k8s/secrets.yaml holds live tokens and must never be committed"


def test_secrets_example_ships_empty_values():
    for line in (K8S / "secrets.example.yaml").read_text().splitlines():
        key, _, value = line.strip().partition(":")
        if key in SECRET_KEYS:
            value = value.split("#", 1)[0].strip()   # the example annotates each key
            assert value == '""', f"{key} must ship empty in secrets.example.yaml"


def test_gitignore_covers_the_secret_files():
    ignored = (ROOT / ".gitignore").read_text()
    for path in (".env", "deploy/k8s/secrets.yaml"):
        assert path in ignored, f"{path} must be gitignored"
