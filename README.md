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
| `PROBE_TEMPLATE_ID`   | Template whose active build configurations the probe checks for. default `CDRelease` |
| `PROBE_INTERVAL`      | Seconds between probes. default 900                              |
| `PROBE_TIMEOUT`       | HTTP timeout in seconds for a probe request. default 60          |

## Logging

Logging is configured through [octopus-oc-corelibs-logging](https://github.com/octopusden/octopus-oc-corelibs-logging)
(`oc-logging`, structlog-based). Every record carries the level, the message, a UTC timestamp
and the calling function name:

```json
{"level": "info", "message": "Start teamcity exporter", "timestamp": "2025-10-09 15:05:43", "func_name": "<module>"}
```

Records coming from third-party libraries (`urllib3`, `requests`) are rendered in the same
one-line format and carry an extra `logger` field with the library logger name. This matters for
log collectors: a bare, multi-line library message gets merged into the preceding record and
breaks JSON parsing on ingest.

```json
{"level": "debug", "message": "Starting new HTTPS connection (1): teamcity.example.com:443", "timestamp": "2025-10-09 15:05:43", "func_name": "_new_conn", "logger": "urllib3.connectionpool"}
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

## Probe

The build metrics above are refreshed in background loops, and a loop that fails keeps serving the
last values it had. An expired token, lost permissions or a full Prometheus disk therefore leave
`up` at 1 and the series in place while the numbers stop being true.

To make that visible, the exporter asks TeamCity a question with a known answer every
`PROBE_INTERVAL` seconds, starting at startup: does template `PROBE_TEMPLATE_ID` have at least one
active build configuration? The counter below increases only when the answer is yes. An error, a
timeout, or an empty answer leave it unchanged and are logged.

```text
teamcity_exporter_probe_success_total << number of successful probes since the exporter started >>
```

Alert when it stops increasing, and when it is missing:

```promql
increase(teamcity_exporter_probe_success_total[45m]) == 0
or absent_over_time(teamcity_exporter_probe_success_total[30m])
```
