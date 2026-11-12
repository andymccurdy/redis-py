# https://github.com/pyinvoke/invoke/issues/833
import inspect
import os
import shutil

from invoke import run, task

if not hasattr(inspect, "getargspec"):
    inspect.getargspec = inspect.getfullargspec

JUNIT_RESULTS_DIR = "junit-results"


def _ensure_junit_results_dir():
    """Create the directory that holds the pytest ``--junit-xml`` reports."""
    os.makedirs(JUNIT_RESULTS_DIR, exist_ok=True)


@task
def devenv(c, endpoints="all"):
    """Brings up the test environment, by wrapping docker compose."""
    clean(c)
    cmd = f"docker compose --profile {endpoints} up -d --build"
    run(cmd)


@task
def build_docs(c):
    """Generates the sphinx documentation."""
    run("pip install -r docs/requirements.txt")
    run("make -C docs html")


@task
def linters(c):
    """Run code linters"""
    run("ruff check tests redis")
    run("ruff format --check --diff tests redis")
    run("vulture redis whitelist.py --min-confidence 80")


@task
def linters_fix(c):
    """Run code linters and fix issues"""
    run("ruff check --fix tests redis")
    run("ruff format tests redis")


@task
def all_tests(c):
    """Run all linters, and tests in redis-py."""
    linters(c)
    tests(c)


def _protocol_arg(protocol):
    """Return ``--protocol=…`` for pytest; empty when no override is wanted."""
    return f"--protocol={protocol}" if protocol else ""


def _protocol_tag(protocol):
    """Return artifact filename suffix for the protocol axis."""
    return f"resp{protocol}" if protocol else "respdefault"


def _legacy_arg(legacy_responses):
    """Return ``--legacy-responses=…`` for pytest, accepting bool or string."""
    if isinstance(legacy_responses, bool):
        value = "true" if legacy_responses else "false"
    else:
        value = legacy_responses
    return f"--legacy-responses={value}"


def _legacy_tag(legacy_responses):
    """Return artifact filename suffix for the ``legacy_responses`` axis."""
    if isinstance(legacy_responses, bool):
        return "_legacy_responses" if legacy_responses else "_unified_responses"
    value = str(legacy_responses).lower()
    if value == "true":
        return "_legacy_responses"
    if value == "false":
        return "_unified_responses"
    return f"_{value}"


@task
def tests(c, uvloop=False, protocol="", legacy_responses=True, profile=False):
    """Run the redis-py test suite against the current python."""
    print("Starting Redis tests")
    fixed_client_tests(c, uvloop=uvloop, profile=profile)
    standalone_tests(
        c,
        uvloop=uvloop,
        protocol=protocol,
        legacy_responses=legacy_responses,
        profile=profile,
    )
    cluster_tests(
        c,
        uvloop=uvloop,
        protocol=protocol,
        legacy_responses=legacy_responses,
        profile=profile,
    )


@task
def fixed_client_tests(c, uvloop=False, profile=False):
    """Run tests that use the fixed client fixture."""
    _ensure_junit_results_dir()
    profile_arg = "--profile" if profile else ""
    # Ignore the scenario suites explicitly, as standalone_tests and cluster_tests
    # do. `-m fixed_client` deselects after collection, so without this pytest still
    # imports them - and the maint-notification scenario modules call
    # generate_params() inside @pytest.mark.parametrize, which queries the fault
    # injector at class-definition time and fails collection where none is running.
    ignore_scenario = (
        "--ignore=tests/test_scenario --ignore=tests/test_asyncio/test_scenario"
    )
    if uvloop:
        run(
            f"pytest {profile_arg} {ignore_scenario} --uvloop --cov=./ --cov-report=xml:coverage_fixed_client_uvloop.xml --junit-xml=junit-results/fixed_client-uvloop-results.xml -m fixed_client"
        )
    else:
        run(
            f"pytest {profile_arg} {ignore_scenario} --cov=./ --cov-report=xml:coverage_fixed_client.xml --junit-xml=junit-results/fixed_client-results.xml -m fixed_client"
        )


@task
def standalone_tests(
    c,
    uvloop=False,
    protocol="",
    legacy_responses=True,
    profile=False,
    redis_mod_url=None,
    extra_markers="",
):
    """Run tests against a standalone redis instance"""
    _ensure_junit_results_dir()
    profile_arg = "--profile" if profile else ""
    redis_mod_url = f"--redis-mod-url={redis_mod_url}" if redis_mod_url else ""
    extra_markers = f" and {extra_markers}" if extra_markers else ""
    protocol_arg = _protocol_arg(protocol)
    protocol_tag = _protocol_tag(protocol)
    legacy_arg = _legacy_arg(legacy_responses)
    legacy_tag = _legacy_tag(legacy_responses)

    if uvloop:
        run(
            f"pytest {profile_arg} {protocol_arg} {legacy_arg} {redis_mod_url}  --ignore=tests/test_scenario --ignore=tests/test_asyncio/test_scenario --cov=./ --cov-report=xml:coverage_{protocol_tag}{legacy_tag}_uvloop.xml -m 'not onlycluster and not fixed_client and not multidb_integration{extra_markers}' --uvloop --junit-xml=junit-results/standalone-{protocol_tag}{legacy_tag}-uvloop-results.xml"
        )
    else:
        run(
            f"pytest {profile_arg} {protocol_arg} {legacy_arg} {redis_mod_url}  --ignore=tests/test_scenario --ignore=tests/test_asyncio/test_scenario --cov=./ --cov-report=xml:coverage_{protocol_tag}{legacy_tag}.xml -m 'not onlycluster and not fixed_client and not multidb_integration{extra_markers}' --junit-xml=junit-results/standalone-{protocol_tag}{legacy_tag}-results.xml"
        )


@task
def cluster_tests(c, uvloop=False, protocol="", legacy_responses=True, profile=False):
    """Run tests against a redis cluster"""
    _ensure_junit_results_dir()
    profile_arg = "--profile" if profile else ""
    cluster_url = "redis://localhost:16379/0"
    cluster_tls_url = "rediss://localhost:27379/0"
    protocol_arg = _protocol_arg(protocol)
    protocol_tag = _protocol_tag(protocol)
    legacy_arg = _legacy_arg(legacy_responses)
    legacy_tag = _legacy_tag(legacy_responses)
    if uvloop:
        run(
            f"pytest {profile_arg} {protocol_arg} {legacy_arg}  --ignore=tests/test_scenario --ignore=tests/test_asyncio/test_scenario  --cov=./ --cov-report=xml:coverage_cluster_{protocol_tag}{legacy_tag}_uvloop.xml -m 'not onlynoncluster and not redismod and not fixed_client and not multidb_integration' --redis-url={cluster_url} --redis-ssl-url={cluster_tls_url} --junit-xml=junit-results/cluster-{protocol_tag}{legacy_tag}-uvloop-results.xml --uvloop"
        )
    else:
        run(
            f"pytest  {profile_arg} {protocol_arg} {legacy_arg}  --ignore=tests/test_scenario --ignore=tests/test_asyncio/test_scenario  --cov=./ --cov-report=xml:coverage_cluster_{protocol_tag}{legacy_tag}.xml -m 'not onlynoncluster and not redismod and not fixed_client and not multidb_integration' --redis-url={cluster_url} --redis-ssl-url={cluster_tls_url} --junit-xml=junit-results/cluster-{protocol_tag}{legacy_tag}-results.xml"
        )


@task
def multidb_integration_tests(c, uvloop=False, profile=False):
    """Run the multi-database (active-active) integration tests.

    These exercise ``MultiDBClient`` end to end against two real Redis
    deployments - both as a double-cluster setup (``cluster`` + ``cluster2``)
    and as a double-standalone setup (``redis`` + ``redis-stack``).

    Requires the docker test environment to be running with the ``multidb``
    profile, e.g. ``invoke devenv --endpoints multidb``, so that both clusters
    (16379, 16385) and both standalone servers (6379, 6479) are reachable.
    Variants whose endpoints are not reachable are skipped.
    """
    _ensure_junit_results_dir()
    profile_arg = "--profile" if profile else ""
    uvloop_arg = "--uvloop" if uvloop else ""
    run(
        f"pytest {profile_arg} {uvloop_arg} -m multidb_integration "
        f"tests/test_multidb tests/test_asyncio/test_multidb "
        f"--junit-xml=junit-results/multidb-integration-results.xml"
    )


@task
def run_test_matrix(c, uvloop=False, profile=False):
    """Run linters and the full ``protocol`` × ``legacy_responses`` matrix.

    Linters and the fixed-client suite run once up front, then the
    standalone and cluster suites are exercised against every combination
    of ``protocol`` ∈ {2, 3, default} and ``legacy_responses`` ∈ {True, False}.
    """
    linters(c)
    fixed_client_tests(c, uvloop=uvloop, profile=profile)
    for protocol in ("2", "3", ""):
        for legacy_responses in (True, False):
            standalone_tests(
                c,
                uvloop=uvloop,
                protocol=protocol,
                legacy_responses=legacy_responses,
                profile=profile,
            )
            cluster_tests(
                c,
                uvloop=uvloop,
                protocol=protocol,
                legacy_responses=legacy_responses,
                profile=profile,
            )


@task
def clean(c):
    """Stop all dockers, and clean up the built binaries, if generated."""
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")
    run("docker compose --profile all rm -s -f")


@task
def package(c):
    """Create the python packages"""
    run("python -m build .")
