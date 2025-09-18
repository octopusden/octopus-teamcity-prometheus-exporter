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
| `METRICS_PORT`     | Set needed port for scrape metrics. default 8000                 |
| `SCRAPE_INTERVAL`     | Set needed interval scrape. default 6000                         |

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
