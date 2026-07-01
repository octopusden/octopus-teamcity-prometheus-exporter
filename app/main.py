import os
import time
import requests
import threading
from prometheus_client import start_http_server, Gauge, Summary
import logging
from datetime import datetime, timedelta, timezone
import json
import re
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_log_level():
    """
    Resolve the numeric logging level from the LOG_LEVEL environment variable.

    If LOG_LEVEL is unset, returns logging.INFO. If LOG_LEVEL is a decimal numeric string, returns its int value. If LOG_LEVEL is a named level (e.g. "debug", "WARNING"), returns the corresponding attribute from the logging module; if the name is unrecognized, returns logging.INFO.

    Returns:
        int: The resolved logging level value (e.g. logging.INFO).
    """
    lvl = os.environ.get("LOG_LEVEL")
    if lvl is None:
        return logging.INFO

    if lvl.isdigit():
        return int(lvl)
    return getattr(logging, lvl.upper(), logging.INFO)


log_level = get_log_level()
log_format = "%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=log_level)
logging.info("Start teamcity exporter")
logging.info(f"{log_level}")

TEAMCITY_URL = os.environ.get("TEAMCITY_URL")
TOKEN = os.environ.get("TEAMCITY_TOKEN")
TEMPLATE_IDS = os.environ.get("TEAMCITY_TEMPLATE_IDS", "")
TEMPLATE_IDS = [tid.strip() for tid in TEMPLATE_IDS.split(",") if tid.strip()]
START_PROJECT_CHAIN = os.environ.get("START_PROJECT_CHAIN", "")
START_PROJECT_CHAIN = [tid.strip() for tid in START_PROJECT_CHAIN.split(",") if tid.strip()]
STOP_PROJECT_CHAIN = os.environ.get("STOP_PROJECT_CHAIN", "")
STOP_PROJECT_CHAIN = [tid.strip() for tid in STOP_PROJECT_CHAIN.split(",") if tid.strip()]
JDK_PROJECT_ID = os.environ.get("JDK_PROJECT_ID")
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", 84600))
STATUS_SCRAPE_INTERVAL = int(os.environ.get("STATUS_SCRAPE_INTERVAL", 1800))  # 30 minutes by default
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
# Per-request HTTP timeout (seconds) for all TeamCity REST calls. Large subtrees (e.g. the whole
# RDDepartment over a 7d window) hit slow deep-pagination pages, so this is generous by default.
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "600"))  # 10 minutes
# How many times to retry a REST call that hits a transient timeout/connection error, so one
# slow page doesn't abort a whole multi-minute failed-builds cycle.
REQUEST_RETRIES = int(os.environ.get("REQUEST_RETRIES", "2"))

# --- Failed-builds-by-meta-runner feature (mirrors tc-build-steps-monitoring fetch) ---
# Scan the PARENT_PROJECT_ID subtree for builds that FAILED in the last WINDOW_DAYS, keep only
# configs that run a monitored meta-runner (recipe), dedup per (config, branch) -> latest failure.
PARENT_PROJECT_ID = os.environ.get("PARENT_PROJECT_ID", JDK_PROJECT_ID or "")
META_RUNNER_IDS = [m.strip() for m in os.environ.get("META_RUNNER_IDS", "").split(",") if m.strip()]
RECIPES_PROJECT_ID = os.environ.get("RECIPES_PROJECT_ID", "") or PARENT_PROJECT_ID
EXCLUDE_PROJECT_IDS = [p.strip() for p in os.environ.get("EXCLUDE_PROJECT_IDS", "").split(",") if p.strip()]
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "7"))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
FAILED_BUILDS_SCRAPE_INTERVAL = int(os.environ.get("FAILED_BUILDS_SCRAPE_INTERVAL", "86400"))  # 24 hours
# Parallelism for the per-(config, branch) recovery check. Matches monit-tc's MAX_WORKERS.
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))

# Recipe (meta-runner) id as it appears on the project recipe admin page: editRecipeId=<id>.
_RECIPE_ID_RE = re.compile(r"editRecipeId=([A-Za-z0-9_.\-]+)")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

BUILD_STATUS_GAUGE = Gauge(
    "teamcity_last_build_status",
    "Last build status for build configurations from a template",
    ["build_type_name", "template_id", "build_type_id", "build_url"]
)

BUILD_DURATION_GAUGE = Gauge(
    "teamcity_last_build_duration_seconds",
    "TeamCity last SUCCESS build duration in seconds",
    ["build_type_name", "template_id", "build_type_id", "build_url"]
)

PROJECT_DURATION_GAUGE = Gauge(
    "teamcity_project_duration_seconds",
    "TeamCity project duration in seconds",
    ["projectId", "project_url", "project_name", "finished_number"]
)

TOTAL_BUILD_CONFIGS_GAUGE = Gauge(
    "teamcity_total_build_configurations",
    "Total number of build configurations in TeamCity"
)

JDK_BUILD_CONFIGS_GAUGE = Gauge(
    "teamcity_jdk_build_configurations",
    "Number of build configurations per JDK version",
    ["jdk_version"]
)

# One series per (config, branch) whose LATEST build in the window is a failure, for configs
# that run a monitored meta-runner. Value is always 1; absence means "not currently failing".
# Mirrors the fetch+dedup of tc-build-steps-monitoring (monit-tc).
#
# Identity labels are STABLE across builds (no build number/url) so a config that stays red
# is ONE continuous series -> smooth graphs and `for:`-based alerts work. The volatile
# per-build detail (number + direct url) lives in FAILED_BUILD_INFO and is joined at query
# time on (build_type_id, branch).
FAILED_BUILD_GAUGE = Gauge(
    "teamcity_failed_build",
    "Currently-failing (config, branch) in the last WINDOW_DAYS for configs using a "
    "monitored meta-runner. Value=1 means currently failing.",
    ["build_type_id", "build_type_name", "project_name", "branch", "build_url", "meta_runner_ids"]
)

# Info metric: latest failing build's number + direct URL per (config, branch). Value always 1.
# NOT for alerting/graphing (build_number churns) — it's a lookup joined to FAILED_BUILD via
#   teamcity_failed_build * on(build_type_id,branch) group_left(build_number,build_url) teamcity_failed_build_info
FAILED_BUILD_INFO = Gauge(
    "teamcity_failed_build_info",
    "Detail (build number + direct URL) of the latest failing build per (config, branch).",
    ["build_type_id", "branch", "build_number", "build_url"]
)


def _tc_get_json(path, params=None, timeout=None):
    """
    Fetch JSON from the TeamCity REST API for the given path and return the parsed response.

    Parameters:
        path (str): API path appended to the configured TeamCity base URL (for example, "/app/rest/builds").
        params (dict|None): Query parameters to include in the request.
        timeout (int|float): Request timeout in seconds.

    Returns:
        The parsed JSON response (typically a dict or list).

    Raises:
        requests.HTTPError: If the HTTP response status indicates an error.
        requests.RequestException: For other request-related errors (connection, timeout, etc.).
    """
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    url = f"{TEAMCITY_URL.rstrip('/')}{path}"
    # Retry transient timeouts/connection errors (a single slow page shouldn't abort a whole
    # multi-minute cycle). HTTP errors (401/404/...) are NOT retried -- they re-raise at once.
    last_exc = None
    for attempt in range(REQUEST_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < REQUEST_RETRIES:
                logging.warning(f"Transient error on {path} ({e}); retry {attempt + 1}/{REQUEST_RETRIES}")
                time.sleep(2 * (attempt + 1))
    raise last_exc


# ===== Failed-builds-by-meta-runner (mirrors tc-build-steps-monitoring) =====

def _tc_paged(path, item_key, params=None):
    """Yield items from a paged TeamCity collection, following nextHref.

    ``item_key`` is the singular element name (e.g. 'build', 'buildType'). nextHref already
    encodes locator + start/count, so params are only sent on the first request.
    """
    next_path = path
    next_params = dict(params or {})
    while next_path:
        data = _tc_get_json(next_path, params=next_params)
        for item in data.get(item_key, []) or []:
            yield item
        next_path = data.get("nextHref") or None
        next_params = None


def get_recipe_ids(project_id):
    """Discover meta-runner (recipe) ids from a project's recipe admin page.

    There is no REST endpoint for recipes, so we scrape /admin/editProject.html and extract
    ``editRecipeId=<id>``. Needs project-settings view access; in prod the service account
    usually lacks it, so this fails gracefully and we fall back to META_RUNNER_IDS.
    """
    url = f"{TEAMCITY_URL.rstrip('/')}/admin/editProject.html?projectId={project_id}&tab=recipe"
    r = requests.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return sorted(set(_RECIPE_ID_RE.findall(r.text)))


def resolve_meta_runner_ids():
    """Monitored set = auto-discovered recipe ids UNION explicit META_RUNNER_IDS."""
    discovered = []
    try:
        discovered = get_recipe_ids(RECIPES_PROJECT_ID)
    except Exception as e:
        logging.warning(f"Recipe discovery from {RECIPES_PROJECT_ID} failed: {e}")
    combined = sorted(set(discovered) | set(META_RUNNER_IDS))
    logging.info(
        f"Monitoring {len(combined)} meta-runner(s): {len(discovered)} discovered "
        f"+ {len(META_RUNNER_IDS)} explicit"
    )
    return combined


def enumerate_candidate_configs(meta_runner_ids):
    """All configs in the PARENT_PROJECT_ID subtree that have an ENABLED step whose runType
    (step.type) is one of the monitored meta-runner ids. Keyed by build config id.
    """
    meta_set = set(meta_runner_ids)
    excluded = set(EXCLUDE_PROJECT_IDS)
    params = {
        "locator": f"affectedProject:(id:{PARENT_PROJECT_ID})",
        "fields": "buildType(id,name,projectName,webUrl,projectId,steps(step(id,name,type,disabled))),nextHref",
        "count": str(PAGE_SIZE),
    }
    configs = {}
    for bt in _tc_paged("/app/rest/buildTypes", "buildType", params):
        if bt.get("projectId") in excluded:
            continue
        steps = (bt.get("steps") or {}).get("step", []) or []
        monitored = sorted({
            s.get("type")
            for s in steps
            if s.get("type") in meta_set and s.get("disabled") is not True
        })
        if monitored:
            configs[bt.get("id")] = {
                "name": bt.get("name", ""),
                "project_name": bt.get("projectName", ""),
                "web_url": bt.get("webUrl", ""),
                "meta_runner_ids": ",".join(monitored),
            }
    return configs


def iter_failed_builds(since):
    """Failed builds (all branches) in the subtree that finished after ``since`` (a tz-aware
    datetime). Same locator as monit-tc's iter_failed_builds.
    """
    date_str = since.strftime("%Y%m%dT%H%M%S%z")
    locator = (
        f"affectedProject:(id:{PARENT_PROJECT_ID}),"
        "status:FAILURE,"
        "branch:(default:any),"
        f"finishDate:(date:{date_str},condition:after)"
    )
    params = {
        "locator": locator,
        "fields": "build(id,number,buildTypeId,branchName,webUrl,finishDate),nextHref",
        "count": str(PAGE_SIZE),
    }
    return _tc_paged("/app/rest/builds", "build", params)


def _branch_locator(branch_name):
    """A branch locator dimension, base64-encoding the name so special characters
    (commas, colons, parens, slashes) can't break the locator syntax. Mirrors monit-tc.
    """
    if not branch_name or branch_name == "<default>":
        return "branch:(default:true)"
    b64 = base64.urlsafe_b64encode(branch_name.encode("utf-8")).decode("ascii").rstrip("=")
    return f"branch:(name:($base64:{b64}))"


def latest_build_on_branch(config_id, branch_name):
    """The most recent build on a given branch (ANY status), or None.

    Used to check whether a (config, branch) that failed within the window has since
    RECOVERED. Mirrors monit-tc's latest_build_on_branch used by compute_current_failures.
    """
    data = _tc_get_json("/app/rest/builds", params={
        "locator": f"buildType:(id:{config_id}),{_branch_locator(branch_name)},count:1",
        "fields": "build(id,number,status,buildTypeId,branchName,webUrl,finishDate)"
    })
    builds = data.get("build") or []
    return builds[0] if builds else None


def _check_still_failing(key, windowed_failure):
    """Recovery check for one (config, branch). Returns (key, build_to_expose_or_None):
    the current build if latest is still FAILURE; the windowed failure on API error
    (fail-safe: don't drop a real failure); None if recovered/gone.
    """
    btid, branch = key
    try:
        newest = latest_build_on_branch(btid, branch)
    except Exception as e:
        logging.warning(f"Latest-build check failed for {btid}@{branch}: {e}; keeping as failing")
        return key, windowed_failure
    if newest is None or newest.get("status") != "FAILURE":
        return key, None  # recovered (latest build is green) or gone -> not currently failing
    return key, newest  # newest IS the current red build


def update_failed_build_metrics(meta_runner_ids):
    """Refresh FAILED_BUILD_GAUGE: one series per (candidate config, branch) that is
    CURRENTLY failing -- i.e. failed within the window AND whose latest build on that branch
    is still FAILURE (not yet recovered). Mirrors monit-tc's fetch+dedup+compute_current_failures.
    """
    configs = enumerate_candidate_configs(meta_runner_ids)
    logging.info(f"Candidate configs (use a monitored meta-runner): {len(configs)}")

    since = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)

    # Dedup per (build_type_id, branch) -> latest by finishDate (fixed-width => lexicographic
    # compare == chronological), matching monit-tc.partition_builds.
    latest = {}
    for b in iter_failed_builds(since):
        btid = b.get("buildTypeId", "")
        if btid not in configs:
            continue  # not a candidate config (doesn't run our meta-runner)
        branch = b.get("branchName", "") or "<default>"
        key = (btid, branch)
        prev = latest.get(key)
        if prev is None or (b.get("finishDate", "") > prev.get("finishDate", "")):
            latest[key] = b

    # Recovery check (mirrors monit-tc.compute_current_failures): a (config, branch) counts as
    # failing only if its LATEST build on that branch is still FAILURE. iter_failed_builds
    # returns FAILURE builds only, so without this we would keep configs that already went
    # green again within the window. One query per failing (config, branch) -> run in parallel
    # (MAX_WORKERS) since at large subtree scale a serial pass is prohibitively slow.
    current = {}
    if latest:
        logging.info(f"Recovery check on {len(latest)} (config, branch) with {MAX_WORKERS} workers")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(_check_still_failing, key, b) for key, b in latest.items()]
            for fut in as_completed(futures):
                key, build = fut.result()
                if build is not None:
                    current[key] = build

    FAILED_BUILD_GAUGE.clear()
    FAILED_BUILD_INFO.clear()
    for (btid, branch), b in current.items():
        c = configs[btid]
        # Stable identity -> continuous series while the config stays red.
        FAILED_BUILD_GAUGE.labels(
            build_type_id=btid,
            build_type_name=c["name"],
            project_name=c["project_name"],
            branch=branch,
            build_url=c["web_url"],  # config page (stable), NOT the per-build url
            meta_runner_ids=c["meta_runner_ids"],
        ).set(1)
        # Volatile per-build detail, joined back on (build_type_id, branch) at query time.
        FAILED_BUILD_INFO.labels(
            build_type_id=btid,
            branch=branch,
            build_number=b.get("number", ""),
            build_url=b.get("webUrl", "") or c["web_url"],
        ).set(1)
    logging.info(
        f"Currently-failing (config+branch, latest build still red) exposed: {len(current)} "
        f"(of {len(latest)} that failed within the {WINDOW_DAYS}d window)"
    )


def fetch_and_update_failed_builds():
    """Continuously refresh the failed-builds-by-meta-runner metric."""
    logging.info("Starting failed-builds (meta-runner) update thread")
    while True:
        try:
            meta_runner_ids = resolve_meta_runner_ids()
            if not meta_runner_ids:
                logging.warning(
                    "No meta-runner ids (discovery empty and META_RUNNER_IDS unset); "
                    "skipping failed-builds update"
                )
            else:
                update_failed_build_metrics(meta_runner_ids)
        except Exception as e:
            logging.error(f"Error in failed-builds update: {e}")

        logging.info(f"Sleeping for {FAILED_BUILDS_SCRAPE_INTERVAL} seconds until next failed-builds update")
        time.sleep(FAILED_BUILDS_SCRAPE_INTERVAL)


def get_build_configs_from_template(template_id):
    """
    Retrieve build configurations associated with a TeamCity template that are not paused.

    Parameters:
        template_id (str): TeamCity template identifier to query.

    Returns:
        list: List of build configuration objects from the TeamCity API; empty list if none are found.

    Raises:
        requests.HTTPError: If the HTTP request to the TeamCity API fails.
    """
    logging.debug("Reached get_build_configs_from_template")

    locator = f"template:{template_id},paused:false"
    data = _tc_get_json("/app/rest/buildTypes", params={
        "locator": locator
    })
    return data.get("buildType", [])


def get_archived_projects():
    """
    Retrieve archived TeamCity project IDs.

    Queries TeamCity for projects marked as archived and returns their IDs.

    Returns:
        list[str]: List of archived project IDs (empty list if none).
    """
    logging.debug("Reached get_archived_projects")
    locator = f"archived:true"
    data = _tc_get_json("/app/rest/projects", params={
        "locator": locator
    })
    return [p['id'] for p in data.get('project', [])]


def get_last_build_status(build_type_id):
    """
    Fetches the most recent build for a TeamCity build configuration.

    Parameters:
        build_type_id (str): TeamCity build configuration (build type) identifier.

    Returns:
        dict: The most-recent build object as returned by the TeamCity API, or `{'status': 'NO_BUILDS'}` if no builds exist.

    Raises:
        requests.HTTPError: If the HTTP request to the TeamCity API returns an error status.
    """
    logging.debug("Reached get_last_build_status")
    locator = f"buildType:{build_type_id},count:1"
    data = _tc_get_json("/app/rest/builds", params={
        "locator": locator,
        "fields": "build(id,number,startDate,finishDate,status,buildTypeId,webUrl,taskId,state,composite)"
    })
    last_build = data.get("build")
    if not last_build:
        return {'status': 'NO_BUILDS'}
    return last_build[0]


def build_duration_seconds(build):
    """
    Compute the duration in seconds between a build's start and finish timestamps.

    Parameters:
        build (dict): Build object containing 'startDate' and 'finishDate' strings in the format "%Y%m%dT%H%M%S%z" (example: "20240102T150405+0000").

    Returns:
        int or None: Number of seconds from start to finish, or `None` if either timestamp is missing.
    """
    start_raw = build.get('startDate')
    finish_raw = build.get('finishDate')
    if not start_raw or not finish_raw:
        return None
    fmt = "%Y%m%dT%H%M%S%z"
    start = datetime.strptime(start_raw, fmt)
    finish = datetime.strptime(finish_raw, fmt)

    delta = finish - start
    return int(delta.total_seconds())


def get_project_url(projectid):
    """
    Get the TeamCity project's web URL for the given project ID.

    Parameters:
        projectid (str): TeamCity project identifier as used by the REST API.

    Returns:
        str or None: The project's `webUrl` reported by TeamCity, or `None` if the field is absent.

    Raises:
        requests.HTTPError: If the HTTP request to TeamCity returns a non-success status.
    """
    logging.debug("Reached get_project_url")
    data = _tc_get_json(f"/app/rest/projects/id:{projectid}", params={})
    return data.get("webUrl")


def get_upstream_chain_nodes(build_id):
    """
    Retrieve upstream snapshot-dependency build nodes for a TeamCity build.

    Parameters:
        build_id: TeamCity build ID whose upstream (snapshot) dependencies will be inspected.

    Returns:
        A list of build objects each containing `buildTypeId`, `id`, `number`, `startDate`, `finishDate`, and `status`, or `None` if no upstream builds are found.
    """
    locator = f"snapshotDependency:(to:(id:{build_id})),defaultFilter:false"
    data = _tc_get_json("/app/rest/builds", params={
        "locator": locator,
        "fields": "build(buildTypeId,id,number,startDate,finishDate,status)"
    })

    return data.get("build")


def get_template_names_for_build_type_id(build_type_id):
    """
    Finds the first template ID associated with a TeamCity build configuration that is listed in START_PROJECT_CHAIN.

    Parameters:
        build_type_id (str): TeamCity build configuration (build type) identifier.

    Returns:
        str or None: The matching template ID from START_PROJECT_CHAIN if found, otherwise None.
    """
    js = _tc_get_json(f"/app/rest/buildTypes/id:{build_type_id}",
                      params={"fields": "templates(buildType)"})
    templates_list = js.get('templates', {"buildType": []})
    for each_template in templates_list['buildType']:
        if each_template.get('id') in START_PROJECT_CHAIN:
            return each_template.get('id')
    return None


def get_start_date_by_last_build_id(build_id):
    """
    Finds the start date of the upstream dependency for the given build that corresponds to a template listed in START_PROJECT_CHAIN.

    Parameters:
        build_id (str): TeamCity build identifier whose upstream dependencies will be inspected.

    Returns:
        start_date (str): The `startDate` value from the first matching upstream dependency, or `None` if no matching dependency is found.
    """
    nodes = get_upstream_chain_nodes(build_id)
    for each_dependencies in nodes:
        template_name = get_template_names_for_build_type_id(each_dependencies['buildTypeId'])
        if not template_name:
            continue
        else:
            return each_dependencies['startDate']
    return None


def get_all_build_configs():
    """
    Retrieve non-archived build configurations from TeamCity, optionally limited to the configured JDK project and its subprojects.

    Returns:
        list: Build configuration objects as returned by the TeamCity API, filtered to exclude configurations whose projects are archived.

    Raises:
        requests.HTTPError: If the HTTP request to the TeamCity API fails.
    """
    logging.debug("Reached get_all_build_configs")

    archived_projects = get_archived_projects()

    logging.info(f"Filtering build configs for project: {JDK_PROJECT_ID}")
    params = {"fields": "buildType(id,projectId,name,templates(buildType(id)))"}
    params["locator"] = f"affectedProject:(id:{JDK_PROJECT_ID})"

    data = _tc_get_json("/app/rest/buildTypes", params=params)

    all_configs = data.get("buildType", [])
    logging.info(f"All build for project {JDK_PROJECT_ID} count is {len(all_configs)}")
    non_archived_configs = [
        cfg for cfg in all_configs
        if cfg.get('projectId') not in archived_projects
    ]
    logging.info(f"All non archived build for project {JDK_PROJECT_ID} count is {len(non_archived_configs)}")
    return non_archived_configs


def get_jdk_version_for_build_config(build_type_id):
    """
    Get the JDK installation path configured for a TeamCity build configuration.

    Parameters:
        build_type_id (str): TeamCity build configuration identifier.

    Returns:
        str: The `env.JAVA_HOME` parameter value when configured; `'not_set'` if the parameter exists but is empty or missing;
        `'return 404'` if the parameter endpoint returns 404; `'HttpError'` for other HTTP errors; `'error'` for any other failure.
    """
    logging.debug(f"Fetching JDK for build config: {build_type_id}")

    try:
        data = _tc_get_json(
            f"/app/rest/buildTypes/id:{build_type_id}/parameters/env.JAVA_HOME",
            params={}
        )
        java_home = data.get('value', '')

        if java_home:
            return java_home
        return 'not_set'
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return 'return 404'
        logging.warning(f"Failed to get JDK for {build_type_id}: {e}")
        return 'HttpError'
    except Exception as e:
        logging.warning(f"Error getting JDK for {build_type_id}: {e}")
        return 'error'


def update_jdk_metrics():
    """
    Update Prometheus gauges reflecting the total number of build configurations and their distribution by JDK version.

    Collects all non-archived build configurations, sets TOTAL_BUILD_CONFIGS_GAUGE to the total count, and sets JDK_BUILD_CONFIGS_GAUGE for each observed JDK version with the number of configurations using that JDK. Errors encountered while retrieving data are logged and do not raise.
    """
    logging.info("Updating JDK metrics")

    try:
        build_configs = get_all_build_configs()
        total_count = len(build_configs)

        TOTAL_BUILD_CONFIGS_GAUGE.set(total_count)
        JDK_BUILD_CONFIGS_GAUGE.clear()
        logging.info(f"Total build configurations: {total_count}")
        jdk_counts = {}
        for cfg in build_configs:
            jdk_version = get_jdk_version_for_build_config(cfg['id'])
            jdk_counts[jdk_version] = jdk_counts.get(jdk_version, 0) + 1
        for jdk_version, count in jdk_counts.items():
            JDK_BUILD_CONFIGS_GAUGE.labels(jdk_version=jdk_version).set(count)
            logging.info(f"JDK {jdk_version}: {count} build configurations")

    except Exception as e:
        logging.error(f"Error updating JDK metrics: {e}")


def update_build_status_metrics():
    """
    Update only BUILD_STATUS_GAUGE metrics for all build configurations from configured templates.
    This function runs more frequently to provide faster status updates.
    """
    logging.info("Updating build status metrics")

    try:
        archived_projects = get_archived_projects()

        for template_id in TEMPLATE_IDS:
            build_configs = get_build_configs_from_template(template_id)

            for cfg in build_configs:
                if cfg['projectId'] in archived_projects:
                    continue

                last_build = get_last_build_status(cfg["id"])
                status = last_build['status']
                status_value = {"SUCCESS": 1, "FAILURE": 0, "NO_BUILDS": -1}.get(status, -1)

                BUILD_STATUS_GAUGE.labels(
                    template_id=template_id,
                    build_type_name=cfg["name"],
                    build_type_id=cfg["id"],
                    build_url=cfg["webUrl"]
                ).set(status_value)

        logging.info(f"Build status metrics updated successfully")

    except Exception as e:
        logging.error(f"Error updating build status metrics: {e}")


def fetch_and_update_full_metrics():
    """
    Continuously poll TeamCity and refresh ALL Prometheus gauges for builds, projects, and JDK distribution.

    On each interval it retrieves build configurations for the configured templates, skips archived projects,
    records the latest build status and successful build durations, aggregates project durations for finished
    project chains, and updates JDK-related metrics; this function runs indefinitely and sleeps SCRAPE_INTERVAL
    between iterations.
    """
    logging.info("Starting full metrics update thread")

    while True:
        all_projects = {}
        try:
            # Inside try so a transient API error (e.g. 401/network) is caught and retried
            # next interval instead of killing this thread permanently.
            archived_projects = get_archived_projects()
            # Update JDK metrics
            update_jdk_metrics()

            for template_id in TEMPLATE_IDS:
                build_configs = get_build_configs_from_template(template_id)

                for cfg in build_configs:
                    if cfg['projectId'] in archived_projects:
                        continue
                    last_build = get_last_build_status(cfg["id"])
                    status = last_build['status']
                    status_value = {"SUCCESS": 1, "FAILURE": 0, "NO_BUILDS": -1}.get(status, -1)
                    current_project_id = cfg['projectId']
                    if status == 'SUCCESS':
                        duration = build_duration_seconds(last_build)
                        if template_id in STOP_PROJECT_CHAIN:
                            all_projects[current_project_id] = {
                                "startDate": get_start_date_by_last_build_id(last_build['id']),
                                "finishDate": last_build["finishDate"],
                                "project_name": cfg["projectName"],
                                "project_url": get_project_url(current_project_id),
                                "finished_number": last_build["number"],
                                "finish_build_id": last_build["id"]}
                        BUILD_DURATION_GAUGE.labels(
                            template_id=template_id,
                            build_type_name=cfg["name"],
                            build_type_id=cfg["id"],
                            build_url=cfg["webUrl"]).set(duration)

                    BUILD_STATUS_GAUGE.labels(
                        template_id=template_id,
                        build_type_name=cfg["name"],
                        build_type_id=cfg["id"],
                        build_url=cfg["webUrl"]
                    ).set(status_value)

            for k, v in all_projects.items():
                if v['startDate']:
                    full_duration = build_duration_seconds(v)
                    if full_duration:
                        PROJECT_DURATION_GAUGE.labels(
                            projectId=k,
                            project_url=v["project_url"],
                            project_name=v["project_name"],
                            finished_number=v['finished_number']
                        ).set(full_duration)
        except Exception as e:
            logging.error(f"Error in full metrics update: {e}")

        logging.info(f"Sleeping for {SCRAPE_INTERVAL} seconds until next full update")
        time.sleep(SCRAPE_INTERVAL)


def fetch_and_update_status_metrics():
    """
    Continuously poll TeamCity and refresh only BUILD_STATUS_GAUGE metrics.
    This function runs more frequently than the full metrics update.
    """
    logging.info("Starting status metrics update thread")

    while True:
        try:
            update_build_status_metrics()
        except Exception as e:
            logging.error(f"Error in status metrics update: {e}")

        logging.info(f"Sleeping for {STATUS_SCRAPE_INTERVAL} seconds until next status update")
        time.sleep(STATUS_SCRAPE_INTERVAL)


if __name__ == "__main__":
    if not all([TEAMCITY_URL, TOKEN, TEMPLATE_IDS, JDK_PROJECT_ID]):
        _error_txt = "TEAMCITY_URL, TEAMCITY_TOKEN, TEAMCITY_TEMPLATE_IDS and JDK_PROJECT_ID must be set as environment variables"
        logging.info(_error_txt)
        raise EnvironmentError(_error_txt)

    start_http_server(METRICS_PORT)
    logging.info(f"Prometheus metrics server running on :{METRICS_PORT}/metrics")
    logging.info(f"Status metrics interval: {STATUS_SCRAPE_INTERVAL} seconds")
    logging.info(f"Full metrics interval: {SCRAPE_INTERVAL} seconds")

    # Start thread for full metrics (JDK, durations, projects, status)
    full_metrics_thread = threading.Thread(target=fetch_and_update_full_metrics, daemon=True)
    full_metrics_thread.start()

    # Start thread for fast status updates only
    status_metrics_thread = threading.Thread(target=fetch_and_update_status_metrics, daemon=True)
    status_metrics_thread.start()

    # Start thread for failed-builds-by-meta-runner (optional; mirrors tc-build-steps-monitoring)
    if PARENT_PROJECT_ID and (META_RUNNER_IDS or RECIPES_PROJECT_ID):
        logging.info(
            f"Failed-builds feature enabled: parent={PARENT_PROJECT_ID}, "
            f"window={WINDOW_DAYS}d, interval={FAILED_BUILDS_SCRAPE_INTERVAL}s"
        )
        failed_builds_thread = threading.Thread(target=fetch_and_update_failed_builds, daemon=True)
        failed_builds_thread.start()
    else:
        logging.info("Failed-builds feature disabled (set PARENT_PROJECT_ID and META_RUNNER_IDS to enable)")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down exporter")