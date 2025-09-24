import os
import time
import requests
import threading
from prometheus_client import start_http_server, Gauge, Summary
import logging
from datetime import datetime
import json


def get_log_level():
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
    logging.debug("Reached get_build_configs_from_template")
    url = f"{TEAMCITY_URL}/app/rest/buildTypes?locator=template:{template_id},paused:false"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get("buildType", [])


def get_archived_projects():
    logging.debug("Reached get_archived_projects")
    url = f"{TEAMCITY_URL}/app/rest/projects?locator=archived:true"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    return [p['id'] for p in data.get('project', [])]


def get_last_build_status(build_type_id):
    logging.debug("Reached get_last_build_status")
    url = f"{TEAMCITY_URL}/app/rest/builds?locator=buildType:{build_type_id},count:1&fields=build(id,number,startDate,finishDate,status,buildTypeId,webUrl,taskId,state,composite)"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    last_build = resp.json().get("build")
    if not last_build:
        return {'status': 'NO_BUILDS'}
    return last_build[0]


def build_duration_seconds(build):
    fmt = "%Y%m%dT%H%M%S%z"
    start = datetime.strptime(build['startDate'], fmt)
    finish = datetime.strptime(build['finishDate'], fmt)
    delta = finish - start
    return int(delta.total_seconds())


def get_project_url(projectid):
    logging.debug("Reached get_project_url")
    url = f"{TEAMCITY_URL}/app/rest/projects/id:{projectid}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get("webUrl")


def fetch_and_update_metrics():
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
                        all_projects[current_project_id] = {"project_duration": 0,
                                                            "project_name": cfg["projectName"],
                                                            "project_url": current_project_url}
                    project_from_all_project = all_projects.get(current_project_id)
                    if status == 'SUCCESS':
                        duration = build_duration_seconds(last_build)
                        project_from_all_project['project_duration'] += duration
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
                PROJECT_DURATION_GAUGE.labels(
                    projectId=k,
                    project_url=v["project_url"],
                    project_name=v["project_name"]
                ).set(v['project_duration'])
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
