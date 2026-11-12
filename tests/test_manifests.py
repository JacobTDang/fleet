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


# Workload kinds that carry a pod template. Service links inject Docker-style
# FLEET_MCP_PORT / NTFY_PORT env vars that shadow the app's own config.
POD_TEMPLATE_KINDS = frozenset({"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"})


def _manifest_documents(path: Path) -> list[tuple[str, str]]:
    """Split a multi-doc YAML file into (kind, raw) pairs."""
    docs: list[tuple[str, str]] = []
    for raw in path.read_text().split("\n---"):
        kind = ""
        for line in raw.splitlines():
            if line.startswith("kind:"):
                kind = line.split(":", 1)[1].strip()
                break
        if kind:
            docs.append((kind, raw))
    return docs


def test_pod_templates_disable_service_links():
    missing = []
    for path in sorted(K8S.rglob("*.yaml")):
        for kind, raw in _manifest_documents(path):
            if kind not in POD_TEMPLATE_KINDS:
                continue
            if "enableServiceLinks: false" not in raw:
                missing.append(f"{path.relative_to(ROOT)} ({kind})")
    assert missing == [], (
        "pod templates must set enableServiceLinks: false so Kubernetes "
        f"does not shadow app env vars: {missing}"
    )


def _configmap_value(key: str) -> str | None:
    for line in (K8S / "config.yaml").read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or ":" not in stripped:
            continue
        name, _, value = stripped.partition(":")
        if name == key:
            return value.split("#", 1)[0].strip().strip('"')
    return None


def _worker_data_mount() -> str:
    """Mount path of the fleet-core worker's data volume (not hard-coded twice)."""
    in_worker = False
    for line in (K8S / "fleet-core.yaml").read_text().splitlines():
        if "name: worker" in line:
            in_worker = True
            continue
        if in_worker and line.strip().startswith("- name:") and "worker" not in line:
            break
        if in_worker and "mountPath:" in line:
            return line.split("mountPath:", 1)[1].strip().strip("}]'\"")
    raise AssertionError("fleet-core worker data mountPath not found")


def test_claude_config_dir_lives_on_data_volume():
    value = _configmap_value("CLAUDE_CONFIG_DIR")
    assert value is not None, "fleet ConfigMap must set CLAUDE_CONFIG_DIR"
    mount = _worker_data_mount()
    assert value == mount or value.startswith(mount.rstrip("/") + "/"), (
        f"CLAUDE_CONFIG_DIR={value!r} must be under worker mount {mount!r}"
    )
