import os
import time
import requests
import threading
from prometheus_client import start_http_server, Gauge
import logging

if os.environ.get("DEBUG_LVL"):
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
log_format = "%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=log_level)
logging.info("Hello")
logging.info(f"{log_level}")


TEAMCITY_URL = os.environ.get("TEAMCITY_URL")
TOKEN = os.environ.get("TEAMCITY_TOKEN")
TEMPLATE_IDS = os.environ.get("TEAMCITY_TEMPLATE_IDS", "")
TEMPLATE_IDS = [tid.strip() for tid in TEMPLATE_IDS.split(",") if tid.strip()]
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", 600))

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

build_status_gauge = Gauge(
    "teamcity_last_build_status",
    "Last build status for build configurations from a template",
    ["build_type_name", "template_id","build_type_id", "build_url" ]
)

def get_build_configs_from_template(template_id):
    logging.debug("Reached get_build_configs_from_template")
    url = f"{TEAMCITY_URL}/app/rest/buildTypes?locator=template:{template_id},paused:false"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get("buildType", [])

def get_last_build_status(build_type_id):
    logging.debug("Reached get_last_build_status")
    url = f"{TEAMCITY_URL}/app/rest/builds?locator=buildType:{build_type_id},count:1"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    builds = resp.json().get("build", [])
    if builds:
        return builds[0]["status"]
    return "NO_BUILDS"

def fetch_and_update_metrics():
    logging.debug("Reached fetch_and_update_metrics")
    while True:
        try:
            for template_id in TEMPLATE_IDS:
                build_configs = get_build_configs_from_template(template_id)
                for cfg in build_configs:
                    if cfg.get("paused", False):
                        continue
                    status = get_last_build_status(cfg["id"])
                    status_value = {"SUCCESS": 1, "FAILURE": 0, "NO_BUILDS": -1}.get(status, -1)
                    build_status_gauge.labels(
                        template_id=template_id,
                        build_type_name=cfg["name"],
                        build_type_id=cfg["id"],
                        build_url=cfg["webUrl"]
                    ).set(status_value)
        except Exception as e:

            logging.debug(f"Obtaining [ERROR] {e}")
        time.sleep(SCRAPE_INTERVAL)

if __name__ == "__main__":
    if not all([TEAMCITY_URL, TOKEN, TEMPLATE_IDS]):
        _error_txt = "TEAMCITY_URL, TEAMCITY_TOKEN, and TEAMCITY_TEMPLATE_ID must be set as environment variables"
        logging.info(_error_txt)
        raise EnvironmentError(_error_txt)

    start_http_server(8000)
    logging.info("Prometheus metrics server running on :8000/metrics")
    thread = threading.Thread(target=fetch_and_update_metrics)
    thread.start()
