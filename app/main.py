import os
import time
import requests
import threading
from prometheus_client import start_http_server, Gauge, Summary
import logging
from datetime import datetime
import json


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
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", 84600))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

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

def _tc_get_json(path, params=None, timeout=30):
    """
    Fetch JSON from the TeamCity REST API at the given path and return the parsed response.
    
    Parameters:
        path (str): API path appended to the configured TeamCity base URL (e.g. "/app/rest/builds").
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
        build (dict): Build object containing 'startDate' and 'finishDate' timestamp strings in the format "%Y%m%dT%H%M%S%z" (example: "20240102T150405+0000").
    
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
    Retrieve upstream snapshot-dependency build nodes for the specified build.
    
    Parameters:
        build_id (str|int): TeamCity build ID to inspect for upstream (snapshot) dependencies.
    
    Returns:
        list or None: A list of build objects containing `buildTypeId`, `id`, `number`, `startDate`, `finishDate`, and `status` for each upstream node, or `None` if no upstream builds are found.
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
    templates_list = js.get('templates', {"buildType":[]})
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

def fetch_and_update_metrics():
    """
    Poll TeamCity and update Prometheus gauges for build and project metrics.
    
    Runs continuously. On each SCRAPE_INTERVAL cycle it retrieves build configurations for TEMPLATE_IDS, skips archived projects, obtains the latest build for each configuration, updates BUILD_STATUS_GAUGE and BUILD_DURATION_GAUGE for each build, and aggregates finished projects (when a start date is available) into PROJECT_DURATION_GAUGE.
    """
    logging.debug("Reached fetch_and_update_metrics")

    while True:
        archived_projects = get_archived_projects()
        all_projects = {}
        try:
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
                            all_projects[current_project_id] = {"startDate": get_start_date_by_last_build_id(last_build['id']),
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
                    PROJECT_DURATION_GAUGE.labels(
                        projectId=k,
                        project_url=v["project_url"],
                        project_name=v["project_name"],
                        finished_number=v['finished_number']
                    ).set(full_duration)
        except Exception as e:

            logging.debug(f"Obtaining [ERROR] {e}")
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    if not all([TEAMCITY_URL, TOKEN, TEMPLATE_IDS]):
        _error_txt = "TEAMCITY_URL, TEAMCITY_TOKEN, and TEAMCITY_TEMPLATE_IDS must be set as environment variables"
        logging.info(_error_txt)
        raise EnvironmentError(_error_txt)
    start_http_server(METRICS_PORT)
    logging.info(f"Prometheus metrics server running on :{METRICS_PORT}/metrics")
    thread = threading.Thread(target=fetch_and_update_metrics)
    thread.start()