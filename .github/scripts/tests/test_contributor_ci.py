"""Regression checks for the CI entry points available to approved fork PRs."""

import os
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]


def workflow(name):
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


def condition(expression, context):
    expression = expression.strip().removeprefix("${{").removesuffix("}}")

    def resolve(match):
        if match[0].startswith("'"):
            return match[0]
        value = context
        for part in match[0].split("."):
            value = value.get(part, "") if isinstance(value, dict) else ""
        return repr(value)

    expression = re.sub(r"'[^']*'|[A-Za-z_]\w*(?:\.[A-Za-z_][\w-]*)+", resolve, expression)
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression)
    expression = re.sub(
        r"'[^']*'|\b(?:true|false)\b", lambda m: m[0] if m[0].startswith("'") else m[0].title(), expression
    )
    return eval(
        " ".join(expression.split()),
        {"__builtins__": {}},
        {"always": lambda: True, "cancelled": lambda: False, "failure": lambda: False},
    )


def event_context(event, fork=False):
    return {
        "github": {
            "event_name": event,
            "repository": "man-group/ArcticDB",
            "token": "read-only-token",
            "event": {
                "pull_request": {
                    "draft": False,
                    "head": {"repo": {"full_name": "contributor/ArcticDB" if fork else "man-group/ArcticDB"}},
                }
            },
        },
        "needs": {"detect_changes": {"outputs": {"docs_only": "false"}}},
        "secrets": {},
    }


@pytest.mark.parametrize("name", ["build.yml", "build_with_conda.yml", "analysis_workflow.yml"])
def test_reusable_builds_keep_checkout_permissions(name):
    config = workflow(name)
    for job_name, job in config["jobs"].items():
        if job.get("uses", "").startswith("./.github/workflows/"):
            permissions = job.get("permissions", config.get("permissions", {}))
            assert permissions.get("contents") in {"read", "write"}, job_name
            called = workflow(Path(job["uses"]).name)
            for scope, permission in called.get("permissions", {}).items():
                levels = {"none": 0, "read": 1, "write": 2}
                assert levels[permissions.get(scope, "none")] >= levels[permission], (job_name, scope)


@pytest.mark.parametrize(
    "event,fork",
    [
        ("pull_request", True),
        ("pull_request", False),
        ("push", False),
        ("schedule", False),
        ("workflow_dispatch", False),
    ],
)
def test_credentials_are_optional_for_approved_fork_checks(event, fork):
    context = event_context(event, fork)
    for name, job_name in [
        ("build.yml", "publish_pytest_data"),
        ("build_with_conda.yml", "publish_pytest_data"),
        ("analysis_workflow.yml", "get_commits_to_benchmark"),
        ("analysis_workflow.yml", "benchmark_commits"),
    ]:
        job = workflow(name)["jobs"][job_name]
        assert bool(condition(job.get("if", "true"), context)) == (not fork), (name, job_name)
    if event == "pull_request":
        job = workflow("claude-code-review.yml")["jobs"]["claude-review"]
        assert bool(condition(job["if"], context)) == (not fork)
        label_check = workflow("automated_release.check_pr_labels.yml")["jobs"]["check_labels"]["steps"][0]
        assert bool(condition(label_check["with"]["add_comment"], context)) == (not fork)
        checkout = next(
            step
            for step in workflow("analysis_workflow.yml")["jobs"]["run-asv-check-script"]["steps"]
            if step.get("uses", "").startswith("actions/checkout@")
        )
        assert condition(checkout["with"]["token"], context) == "read-only-token"


def test_forks_retain_normal_build_coverage():
    config = workflow("build.yml")
    context = event_context("pull_request", fork=True)
    image = config["jobs"]["cibw_docker_image"]
    assert condition(image["if"], context)
    for os_name in ["linux", "windows", "macos"]:
        for prefix in ["cpp-test", "build-python-wheels"]:
            job = config["jobs"][f"{prefix}-{os_name}"]
            assert condition(job.get("if", "true"), context)
            assert job["uses"] == "./.github/workflows/build_steps.yml"
        wheel = config["jobs"][f"build-python-wheels-{os_name}"]
        assert "[9, 10, 11, 12, 13, 14]" in wheel["strategy"]["matrix"]["python3"]
    for job in ["run-asv-check-script", "sanitizer-address-leak-undefined"]:
        assert condition(workflow("analysis_workflow.yml")["jobs"][job]["if"], context)


@pytest.fixture
def cache_environment(tmp_path):
    (tmp_path / "build_tooling").mkdir()
    shutil.copy(ROOT / "build_tooling/vcpkg_caching.sh", tmp_path / "build_tooling")
    vcpkg = tmp_path / "cpp/vcpkg"
    vcpkg.mkdir(parents=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    commands = {
        vcpkg
        / "vcpkg": '#!/bin/bash\n[[ "${FETCH_EXIT:-0}" == 0 ]] || exit "$FETCH_EXIT"\nprintf "%s\\n" "$STUB_NUGET"\n',
        bin_dir / "nuget": '#!/bin/bash\necho "$1 $2" >> "$COMMAND_LOG"\nexit "${NUGET_EXIT:-0}"\n',
        bin_dir / "curl": '#!/bin/bash\necho curl >> "$COMMAND_LOG"\nexit "${CURL_EXIT:-0}"\n',
        bin_dir / "mono": '#!/bin/bash\nexec "$@"\n',
    }
    for path, script in commands.items():
        path.write_text(script)
        path.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("VCPKG_", "AWS_", "X_VCPKG_"))}
    env.update(
        PATH=f"{bin_dir}:{env['PATH']}", STUB_NUGET=str(bin_dir / "nuget"), COMMAND_LOG=str(tmp_path / "commands")
    )
    return tmp_path, env


@pytest.mark.parametrize("mode", ["off", "read", "readwrite"])
@pytest.mark.parametrize("credentials", [False, True])
@pytest.mark.parametrize("denied", [False, True])
def test_binary_cache_misses_do_not_prevent_builds(cache_environment, mode, credentials, denied):
    directory, env = cache_environment
    env["VCPKG_BINARY_CACHE_MODE"] = mode
    env["CURL_EXIT"] = "22" if denied else "0"
    if credentials:
        env.update(VCPKG_NUGET_USER="test-owner", VCPKG_NUGET_TOKEN="test-token")
    result = subprocess.run(
        ["bash", "-e", "-c", '. build_tooling/vcpkg_caching.sh; printf "SOURCES=%s\\n" "$VCPKG_BINARY_SOURCES"'],
        cwd=directory,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    enabled = mode != "off" and credentials and not denied
    assert f"SOURCES=clear;nuget,github,{mode}" in result.stdout if enabled else "SOURCES=clear\n" in result.stdout
    log_path = directory / "commands"
    commands = log_path.read_text() if log_path.exists() else ""
    assert ("setapikey" in commands) == (enabled and mode == "readwrite")
    if not credentials or mode == "off":
        assert commands == ""
    assert "test-token" not in result.stdout + result.stderr


def test_binary_cache_configuration_failure_falls_back(cache_environment):
    directory, env = cache_environment
    env.update(VCPKG_NUGET_USER="test-owner", VCPKG_NUGET_TOKEN="test-token", NUGET_EXIT="1")
    result = subprocess.run(
        ["bash", "-e", "-c", '. build_tooling/vcpkg_caching.sh; printf "SOURCES=%s\\n" "$VCPKG_BINARY_SOURCES"'],
        cwd=directory,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "SOURCES=clear\n" in result.stdout


def test_optional_nuget_download_failure_is_a_cache_miss(cache_environment):
    directory, env = cache_environment
    env.update(VCPKG_NUGET_USER="test-owner", VCPKG_NUGET_TOKEN="test-token", FETCH_EXIT="1")
    result = subprocess.run(
        [
            "bash",
            "-e",
            "-o",
            "pipefail",
            "-c",
            '. build_tooling/vcpkg_caching.sh; printf "SOURCES=%s\\n" "$VCPKG_BINARY_SOURCES"',
        ],
        cwd=directory,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "SOURCES=clear\n" in result.stdout


@pytest.mark.parametrize(
    "name,job_name", [("build_steps.yml", "compile"), ("analysis_workflow.yml", "run-asv-check-script")]
)
@pytest.mark.parametrize("aws", [False, True])
def test_compilers_select_backend_before_sccache_starts(name, job_name, aws):
    context = event_context("pull_request", fork=not aws)
    if aws:
        context["secrets"] = {"AWS_S3_ACCESS_KEY": "key", "AWS_S3_SECRET_KEY": "secret", "VCPKG_NUGET_TOKEN": "token"}
    context["vars"] = {}
    job = workflow(name)["jobs"][job_name]
    env = job["env"]
    for key in ["SCCACHE_BUCKET", "SCCACHE_ENDPOINT", "SCCACHE_REGION", "SCCACHE_S3_USE_SSL"]:
        assert key not in env, key
    backend_step = next(i for i, step in enumerate(job["steps"]) if "sccache_config.sh" in step.get("run", ""))
    start_step = next(
        i for i, step in enumerate(job["steps"]) if step.get("uses", "").startswith("mozilla-actions/sccache-action@")
    )
    assert backend_step < start_step
    assert condition(env.get("VCPKG_BINARY_CACHE_MODE", "''"), context) == ("readwrite" if aws else "off")
    if name == "build_steps.yml":
        assert "SCCACHE_GHA_ENABLED" in env["CIBW_ENVIRONMENT_PASS_LINUX"].split()
        assert "ACTIONS_CACHE_SERVICE_V2" in env["CIBW_ENVIRONMENT_PASS_LINUX"].split()
        assert "VCPKG_BINARY_CACHE_MODE" in env["CIBW_ENVIRONMENT_PASS_LINUX"].split()


@pytest.mark.parametrize("credentials", ["absent", "both", "only-key", "only-secret"])
def test_sccache_backend_configuration_handles_absent_credentials(tmp_path, credentials):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "SCCACHE_"))}
    env["GITHUB_ENV"] = str(tmp_path / "github-env")
    env.update(SCCACHE_BUCKET="", SCCACHE_S3_USE_SSL="", SCCACHE_GHA_VERSION="false")
    if credentials in {"both", "only-key"}:
        env["AWS_ACCESS_KEY_ID"] = "key"
    if credentials in {"both", "only-secret"}:
        env["AWS_SECRET_ACCESS_KEY"] = "secret"
    script = ROOT / "build_tooling/sccache_config.sh"
    result = subprocess.run(
        ["bash", "-e", "-c", f'. "{script}"; env'],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    settings = dict(line.split("=", 1) for line in result.stdout.splitlines() if line.startswith("SCCACHE_"))
    exported = dict(line.split("=", 1) for line in (tmp_path / "github-env").read_text().splitlines())
    assert exported.pop("ACTIONS_CACHE_SERVICE_V2", "") == "true"
    assert settings == exported
    if credentials == "both":
        assert settings["SCCACHE_BUCKET"] == "arcticdb-ci-sccache-bucket"
        assert settings["SCCACHE_S3_USE_SSL"] == "false"
        assert "SCCACHE_GHA_VERSION" not in settings
    else:
        assert settings == {"SCCACHE_GHA_ENABLED": "true", "SCCACHE_GHA_VERSION": "1"}


@pytest.mark.parametrize("publish", [False, True])
@pytest.mark.parametrize("hit", [False, True])
@pytest.mark.parametrize("force", [False, True])
def test_manylinux_image_cache_miss_keeps_fork_builds_available(tmp_path, publish, hit, force):
    tooling = tmp_path / "build_tooling"
    tooling.mkdir()
    resolver = ROOT / "build_tooling/resolve_cibw_image.sh"
    if resolver.exists():
        shutil.copy(resolver, tooling)
    workflow_dir = tmp_path / ".github/workflows"
    workflow_dir.mkdir(parents=True)
    shutil.copy(ROOT / ".github/workflows/cibw_docker_image.yml", workflow_dir)
    (tooling / "build_many_linux_image.sh").write_text('#!/bin/bash\necho build >> "$COMMAND_LOG"\n')
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/bash\necho "$*" >> "$COMMAND_LOG"\n'
        'if [[ "$1 $2" == "manifest inspect" ]]; then [[ "$DOCKER_HIT" == true ]]; else exit 0; fi\n'
    )
    docker.chmod(0o755)
    output = tmp_path / "output"
    env = dict(
        os.environ,
        PATH=f"{tmp_path}:{os.environ['PATH']}",
        COMMAND_LOG=str(tmp_path / "commands"),
        DOCKER_HIT=str(hit).lower(),
        publish_image=str(publish).lower(),
        force_update=str(force).lower(),
        cibuildwheel_ver="3.1.4",
        REPO_OWNER="man-group",
        ARTIFACT_NAME="arcticdb_manylinux",
        image_override="",
        GH_TOKEN="test-token",
        GITHUB_OUTPUT=str(output),
    )
    result = subprocess.run(
        ["bash", "build_tooling/resolve_cibw_image.sh"], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    commands = (tmp_path / "commands").read_text() if (tmp_path / "commands").exists() else ""
    found = hit and not force
    assert ("build\n" in commands) == (publish and not found)
    assert ("login" in commands) == (publish and not found)
    assert ("push" in commands) == (publish and not found)
    assert f"build_locally={str(not publish and not found).lower()}" in output.read_text()
    assert f"found_existing={'success' if found else 'failure'}" in output.read_text()
    assert "test-token" not in commands + result.stdout + result.stderr


def test_fork_image_fallback_is_built_before_cibuildwheel():
    caller = workflow("build.yml")["jobs"]["build-python-wheels-linux"]
    assert "needs.cibw_docker_image.outputs.build_locally" in caller["with"].get("build_cibw_image", "")
    steps = workflow("build_steps.yml")["jobs"]["compile"]["steps"]
    image_step = next(step for step in steps if step.get("name") == "Get CIBuildWheel image & metadata")
    assert "inputs.build_cibw_image" in image_step["env"].get("BUILD_LOCALLY", "")
    assert "build_tooling/build_many_linux_image.sh" in image_step["run"]
    assert "docker login" not in image_step["run"]


@pytest.mark.parametrize("name,gate_name", [("build.yml", "can_merge"), ("build_with_conda.yml", "can_merge_conda")])
@pytest.mark.parametrize("docs_only", [False, True])
@pytest.mark.parametrize("result", ["success", "failure", "cancelled", "skipped"])
def test_required_gate_rejects_failed_or_unexpectedly_skipped_checks(name, gate_name, docs_only, result):
    gate = workflow(name)["jobs"][gate_name]
    assert "detect_changes" in gate["needs"]
    if name == "build.yml":
        assert "run_linting_checks" in gate["needs"]
        assert "persistent_storage_verify_macos" in gate["needs"]
    jobs = {job: {"result": "success"} for job in gate["needs"]}
    jobs["detect_changes"]["outputs"] = {"docs_only": str(docs_only).lower()}
    jobs.setdefault("storage_type", {"result": "success"})["outputs"] = {"storage": "no"}
    step = gate["steps"][-1]
    build_jobs = step["env"]["CI_BUILD_JOBS"].split()
    for job in step["env"].get("CI_STORAGE_JOBS", "").split():
        jobs[job]["result"] = "skipped"
    if docs_only:
        for job in build_jobs:
            jobs[job]["result"] = "skipped"
    jobs[build_jobs[0]]["result"] = result
    env = dict(
        os.environ, CI_JOB_RESULTS=json.dumps(jobs), **{k: v for k, v in step["env"].items() if k != "CI_JOB_RESULTS"}
    )
    run = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=ROOT, env=env, capture_output=True, text=True)
    assert (run.returncode == 0) == (result == "success" or (docs_only and result == "skipped")), (
        run.stdout + run.stderr
    )


@pytest.mark.parametrize("storage", ["no", "AWS_S3"])
@pytest.mark.parametrize("storage_result", ["success", "failure", "skipped"])
def test_required_gate_validates_persistent_tests(storage, storage_result):
    gate = workflow("build.yml")["jobs"]["can_merge"]
    assert "storage_type" in gate["needs"]
    jobs = {job: {"result": "success"} for job in gate["needs"]}
    jobs["detect_changes"]["outputs"] = {"docs_only": "false"}
    jobs["storage_type"]["outputs"] = {"storage": storage}
    step = gate["steps"][-1]
    for job in step["env"]["CI_STORAGE_JOBS"].split():
        jobs[job]["result"] = storage_result
    env = dict(
        os.environ, CI_JOB_RESULTS=json.dumps(jobs), **{k: v for k, v in step["env"].items() if k != "CI_JOB_RESULTS"}
    )
    run = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=ROOT, env=env, capture_output=True, text=True)
    assert (run.returncode == 0) == (storage_result == "success" or (storage == "no" and storage_result == "skipped"))


def test_ci_script_regressions_run_when_workflow_or_cache_inputs_change():
    config = workflow("test_ci_scripts.yml")
    for event in ["pull_request", "push"]:
        paths = config["on"][event]["paths"]
        assert ".github/workflows/**" in paths
        for script in ["vcpkg_caching.sh", "sccache_config.sh", "resolve_cibw_image.sh", "build_many_linux_image.sh"]:
            assert f"build_tooling/{script}" in paths
    install = next(
        step["run"]
        for step in config["jobs"]["test-scripts"]["steps"]
        if step.get("name") == "Install test dependencies"
    )
    assert "pyyaml" in install


def test_trusted_reporter_retains_artifact_download_permissions():
    assert workflow("publish_pytest_data.yml")["permissions"].get("actions") == "read"
