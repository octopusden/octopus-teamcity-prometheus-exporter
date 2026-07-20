# octopus-teamcity-prometheus-exporter

This exporter is used to retrieve the build status from **TeamCity** for builds that inherit from specific templates and exclude paused.

## Environment Variables

The following environment variables **must** be set when running the exporter:

| Variable                | Description                                   |
|-------------------------|-----------------------------------------------|
| `TEAMCITY_TOKEN`        | Token for connecting to TeamCity              |
| `TEAMCITY_URL`          | URL of the TeamCity instance                  |
| `TEAMCITY_TEMPLATE_IDS` | List of template IDs whose builds are processed |

Optional:

| Variable        | Description                                                      |
|-----------------|------------------------------------------------------------------|
| `LOG_LEVEL`     | Set needed level for logging (number or name), default INFO (20) |
| `LOG_FORMAT`    | Logging output format: `json` (default) or `text`                |
| `METRICS_PORT`     | Set needed port for scrape metrics. default 8000                 |
| `SCRAPE_INTERVAL`     | Set needed interval scrape. default 6000                         |

## Logging

Logging is configured through [octopus-oc-corelibs-logging](https://github.com/octopusden/octopus-oc-corelibs-logging)
(`oc-logging`, structlog-based). Every record carries the level, the message, a UTC timestamp
and the calling function name:

```json
{"level": "info", "message": "Start teamcity exporter", "timestamp": "2025-10-09 15:05:43", "func_name": "<module>"}
```

## Metric Format

The exporter outputs Prometheus metrics in the following format:

```text
teamcity_last_build_status{
  template_id="<< ID template name >>",
  build_type_id="<< ID of the build being checked >>",
  build_type_name="<< name of the specific build >>",
  build_url="<< build URL >>"
} << build result >>
```

### Possible build result values

| Value | Status               |
|----------|-------------------|
| `1`      | Successful build|
| `0`      | Failed build|
| `-1`     | No results |
