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
    Determine the logging level to use based on the LOG_LEVEL environment variable.
    
    If LOG_LEVEL is unset, returns logging.INFO. If LOG_LEVEL is a numeric string, the numeric value is returned as an int. If LOG_LEVEL is a named level (e.g. "debug", "WARNING"), the corresponding attribute from the logging module is returned; if the name is unrecognized, logging.INFO is returned.
    
    Returns:
        int: The resolved logging level value.
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
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", 6000))
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
    ["projectId", "project_url", "project_name"]
)


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
    url = f"{TEAMCITY_URL}/app/rest/buildTypes?locator=template:{template_id},paused:false"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get("buildType", [])


def get_archived_projects():
    """
    Return archived TeamCity project IDs.
    
    Fetches archived projects from the TeamCity REST API and returns a list of their IDs.
    
    Returns:
        list[str]: Project IDs marked as archived (empty list if no archived projects).
    """
    logging.debug("Reached get_archived_projects")
    url = f"{TEAMCITY_URL}/app/rest/projects?locator=archived:true"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
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
    url = f"{TEAMCITY_URL}/app/rest/builds?locator=buildType:{build_type_id},count:1&fields=build(id,number,startDate,finishDate,status,buildTypeId,webUrl,taskId,state,composite)"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    last_build = resp.json().get("build")
    if not last_build:
        return {'status': 'NO_BUILDS'}
    return last_build[0]


def build_duration_seconds(build):
    """
    Compute the duration in seconds between a build's start and finish timestamps.
    
    Parameters:
        build (dict): Build dictionary containing 'startDate' and 'finishDate' as strings in the format "%Y%m%dT%H%M%S%z" (e.g., 20240102T150405+0000).
    
    Returns:
        int: Number of seconds between finish and start timestamps.
    """
    fmt = "%Y%m%dT%H%M%S%z"
    start = datetime.strptime(build['startDate'], fmt)
    finish = datetime.strptime(build['finishDate'], fmt)
    if start == '' or finish == '':
        return None
    delta = finish - start
    return int(delta.total_seconds())


def get_project_url(projectid):
    """
    Fetches the TeamCity project's web URL for the given project ID.
    
    Parameters:
        projectid (str): TeamCity project identifier (project id as used by the REST API).
    
    Returns:
        str or None: The project's `webUrl` as reported by TeamCity, or `None` if the field is absent.
    
    Raises:
        requests.HTTPError: If the HTTP request to TeamCity returns a non-success status.
    """
    logging.debug("Reached get_project_url")
    url = f"{TEAMCITY_URL}/app/rest/projects/id:{projectid}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get("webUrl")


def fetch_and_update_metrics():
    """
    Continuously polls TeamCity and updates Prometheus gauges for build and project durations and build statuses.
    
    Periodically (every SCRAPE_INTERVAL seconds) retrieves build configurations for TEMPLATE_IDS, ignores archived projects, reads each configuration's most recent build status, updates BUILD_STATUS_GAUGE for each build configuration, accumulates durations of successful builds per project and updates BUILD_DURATION_GAUGE for the last successful build and PROJECT_DURATION_GAUGE for the project's cumulative duration. This function runs indefinitely and performs network requests to TeamCity during each cycle.
    """
    logging.debug("Reached fetch_and_update_metrics")

    while True:
        archived_projects = get_archived_projects()
        all_projects = {}
        try:
            for template_id in TEMPLATE_IDS:
                build_configs = get_build_configs_from_template(template_id)
                for cfg in build_configs:
                    if cfg['id'] in archived_projects:
                        continue
                    last_build = get_last_build_status(cfg["id"])
                    status = last_build['status']
                    status_value = {"SUCCESS": 1, "FAILURE": 0, "NO_BUILDS": -1}.get(status, -1)
                    current_project_id = cfg['projectId']
                    project_from_all_project = all_projects.get(current_project_id)
                    if not project_from_all_project:
                        current_project_url = get_project_url(current_project_id)
                        all_projects[current_project_id] = {"startDate": "",
                                                            "finishDate": "",
                                                            "project_name": cfg["projectName"],
                                                            "project_url": current_project_url}
                    project_from_all_project = all_projects.get(current_project_id)
                    if status == 'SUCCESS':
                        duration = build_duration_seconds(last_build)
                        if template_id in ["CDGradleBuild", "CDJavaMavenBuild"]:
                            project_from_all_project["startDate"] = last_build["startDate"]
                        elif template_id in ["CDRelease"]:
                            project_from_all_project["finishDate"] = last_build["finishDate"]
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

            for k,v in all_projects.items():
                full_duration = build_duration_seconds(v)
                if full_duration:
                    PROJECT_DURATION_GAUGE.labels(
                        projectId=k,
                        project_url=v["project_url"],
                        project_name=v["project_name"]
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
