import os
import time
import requests
import threading
from prometheus_client import start_http_server, Gauge, Summary
import logging
from datetime import datetime, timezone


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
BUILD_LIMIT = os.environ.get("BUILD_LIMIT", None)
if BUILD_LIMIT:
    BUILD_LIMIT = int(BUILD_LIMIT)
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

BUILD_STATUS_GAUGE = Gauge(
    "teamcity_last_build_status",
    "Last build status for build configurations from a template",
    ["build_type_name", "template_id", "build_type_id", "build_url"]
)

BUILD_FINISH_DATE_GAUGE = Gauge(
    "teamcity_last_build_finish_date",
    "Last build finish date (unix timestamp) for build configurations from a template",
    ["build_type_name", "template_id", "build_type_id", "build_url"]
)

BUILD_START_DATE_GAUGE = Gauge(
    "teamcity_last_build_start_date",
    "Last build start date (unix timestamp) for build configurations from a template",
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


def _tc_get_json(path, params=None, timeout=30):
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
    url = f"{TEAMCITY_URL.rstrip('/')}{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


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
    return int((finish - start).total_seconds())


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
    params = {
        "fields": "buildType(id,projectId,name,templates(buildType(id)))",
        "locator": f"affectedProject:(id:{JDK_PROJECT_ID})"
    }
    data = _tc_get_json("/app/rest/buildTypes", params=params)
    all_configs = data.get("buildType", [])
    logging.info(f"All build for project {JDK_PROJECT_ID} count is {len(all_configs)}")
    non_archived_configs = [cfg for cfg in all_configs if cfg.get('projectId') not in archived_projects]
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
        return java_home if java_home else 'not_set'
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


def convert_time(dt_str=""):
    try:
        dt = datetime.strptime(dt_str, "%Y%m%dT%H%M%S%z")
        return int(dt.timestamp())
    except (ValueError, TypeError):
        logging.warning(f"Failed to parse timestamp: {dt_str!r}")
        return 0


def _set_build_metrics(template_id, cfg, last_build):
    """
    Helper: sets BUILD_STATUS_GAUGE, BUILD_FINISH_DATE_GAUGE, and BUILD_START_DATE_GAUGE
    for a single build configuration based on the latest build result.
    """
    status = last_build['status']
    status_value = {"SUCCESS": 1, "FAILURE": 0, "NO_BUILDS": -1}.get(status, -1)

    finish_date = convert_time(last_build.get('finishDate', '')) if status != "NO_BUILDS" else 0
    start_date = convert_time(last_build.get('startDate', '')) if status != "NO_BUILDS" else 0

    labels = dict(
        template_id=template_id,
        build_type_name=cfg["name"],
        build_type_id=cfg["id"],
        build_url=cfg["webUrl"],
    )

    BUILD_STATUS_GAUGE.labels(**labels).set(status_value)
    BUILD_FINISH_DATE_GAUGE.labels(**labels).set(finish_date)
    BUILD_START_DATE_GAUGE.labels(**labels).set(start_date)


def update_build_status_metrics():
    """
    Update BUILD_STATUS_GAUGE for all build configurations derived from the configured templates.

    For each non-archived build configuration this function sets BUILD_STATUS_GAUGE with labels
    (template_id, build_type_name, build_type_id, build_url, finish_date, start_date).
    Value semantics: `1` for SUCCESS, `0` for FAILURE, `-1` for NO_BUILDS or unknown status.
    Skip build configurations belonging to archived projects.
    """
    logging.info("Updating build status metrics")
    try:
        archived_projects = get_archived_projects()
        for template_id in TEMPLATE_IDS:
            build_configs = get_build_configs_from_template(template_id)[:BUILD_LIMIT]
            for cfg in build_configs:
                if cfg['projectId'] in archived_projects:
                    continue
                last_build = get_last_build_status(cfg["id"])
                _set_build_metrics(template_id, cfg, last_build)
        logging.info("Build status metrics updated successfully")
    except Exception as e:
        logging.error(f"Error updating build status metrics: {e}")


def fetch_and_update_full_metrics():
    """
    Continuously poll TeamCity and refresh all Prometheus gauges for builds, projects, and JDK distribution.

    This function runs indefinitely: on each iteration it updates JDK-related metrics, iterates configured templates to
    collect non-archived build configurations, records latest build status and successful build durations, aggregates
    project-chain durations for templates in the stop-project chain, and updates project-level duration metrics.
    It skips archived projects and pauses SCRAPE_INTERVAL seconds between iterations.
    """
    logging.info("Starting full metrics update thread")
    while True:
        archived_projects = get_archived_projects()
        all_projects = {}
        try:
            update_jdk_metrics()

            for template_id in TEMPLATE_IDS:
                build_configs = get_build_configs_from_template(template_id)[:BUILD_LIMIT]
                for cfg in build_configs:
                    if cfg['projectId'] in archived_projects:
                        continue

                    last_build = get_last_build_status(cfg["id"])
                    status = last_build['status']
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
                            build_url=cfg["webUrl"],
                        ).set(duration)


                    _set_build_metrics(template_id, cfg, last_build)

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

    # full_metrics_thread = threading.Thread(target=fetch_and_update_full_metrics, daemon=True)
    # full_metrics_thread.start()

    status_metrics_thread = threading.Thread(target=fetch_and_update_status_metrics, daemon=True)
    status_metrics_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down exporter")