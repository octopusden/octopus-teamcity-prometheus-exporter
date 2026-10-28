# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

A Prometheus exporter that polls the TeamCity REST API and exposes build health as metrics.
It is a single Python file, `app/main.py`, packaged as a Docker image
(`python:3.12-slim`, port 8000). There are no tests.

## Run locally

```bash
pip install -r requirements.txt
export TEAMCITY_URL=https://teamcity.example.com TEAMCITY_TOKEN=... \
       TEAMCITY_TEMPLATE_IDS=tmplA,tmplB JDK_PROJECT_ID=SomeProject
python app/main.py          # metrics on :8000/metrics
```
The four variables above are required. The process raises at startup if any is missing.
Use `LOG_FORMAT=text` for readable local logs (the default is `json`).

## Threads (all daemon; the main thread only sleeps)

| Loop | Interval env (default) | Sets |
|---|---|---|
| full metrics | `SCRAPE_INTERVAL` (84600) | JDK distribution, total configs, status, duration, project chain duration |
| status | `STATUS_SCRAPE_INTERVAL` (1800) | status only |
| known-answer probe | `PROBE_INTERVAL` (900), `PROBE_TEMPLATE_ID`, `PROBE_TIMEOUT` | `teamcity_exporter_probe_success_total` |
| failed builds | `FAILED_BUILDS_SCRAPE_INTERVAL` (86400) | `teamcity_failed_build` |

The failed-builds loop is effectively always on. `RECIPES_PROJECT_ID` falls back to
`PARENT_PROJECT_ID`, which falls back to `JDK_PROJECT_ID`.

Other env vars:
- HTTP: `REQUEST_TIMEOUT` (600), `REQUEST_RETRIES` (2; retries on timeout/connection only),
  `PAGE_SIZE` (100).
- Failed-builds scope: `META_RUNNER_IDS`, `EXCLUDE_PROJECT_IDS`, `WINDOW_DAYS` (7),
  `BUILDS_CHUNK_HOURS` (24), `FAILED_BUILDS_CHUNK_COUNT`, `MAX_WORKERS` (8),
  `BUILD_STEP_PATTERNS` ("Build & Publish,Publish").
- Chain duration: `START_PROJECT_CHAIN`, `STOP_PROJECT_CHAIN`.

Every default can be overridden through the environment, so check the deployment's values
before reasoning about timing. All are defined near the top of `app/main.py`. Read them there
rather than trusting README.md, which is partly out of date.

## Metrics are a public contract

Downstream alert rules and dashboards select these names and labels. Renaming or removing a
metric or label, or changing its value semantics, is a **breaking change**: say so in the
PR and the release notes.

| Metric | Labels | Value |
|---|---|---|
| `teamcity_last_build_status` | build_type_name, template_id, build_type_id, build_url | 1 success, 0 failure, -1 other/none |
| `teamcity_last_build_duration_seconds` | same | only set when the last build succeeded |
| `teamcity_project_duration_seconds` | projectId, project_url, project_name, finished_number | chain duration |
| `teamcity_total_build_configurations` | — | count |
| `teamcity_jdk_build_configurations` | jdk_version | count per `env.JAVA_HOME` value (or a sentinel such as `not_set`) |
| `teamcity_failed_build` | build_type_id, build_type_name, project_name, branch, build_url, meta_runner_ids | 1 per currently failing (config, branch); replaced on each completed cycle (see below) |
| `teamcity_exporter_probe_success_total` | — | counter; stops increasing when the exporter can't get correct answers from TeamCity |

Gotchas:
- Which gauges are cleared:
  - `teamcity_failed_build` is cleared and rebuilt only when a failed-builds cycle gets past
    candidate enumeration and the recovery checks. A cycle that is skipped (no meta-runner
    ids resolved) or that raises before that point keeps the previous series.
  - `teamcity_jdk_build_configurations` is cleared and rebuilt in each full cycle that
    fetches the config list, so a JDK version that disappears drops out.
    `teamcity_total_build_configurations` is recomputed in the same step.
  - `teamcity_last_build_status`, `teamcity_last_build_duration_seconds` and
    `teamcity_project_duration_seconds` are never cleared: removed configs keep their last
    value until restart.
- `finished_number` makes `teamcity_project_duration_seconds` grow one series per chain run.
- Archived projects and `EXCLUDE_PROJECT_IDS` subtrees, by update path:
  - Status, duration, project duration and JDK metrics: both are filtered. If the archived
    or excluded lookup fails, the whole cycle is skipped and the previous values stay.
  - Failed builds: the excluded subtree is filtered, and a failure to resolve it skips the
    cycle. If the archived-projects lookup fails, that cycle runs **without** archived
    filtering.
  - Known-answer probe: no filtering. It counts a success whenever `PROBE_TEMPLATE_ID` has
    an active config, even one under an excluded subtree.
  - An excluded project id that returns 404 (gone, renamed or mistyped) is logged and
    ignored. The cycle continues without it.
- Values change only when a loop completes (every 900s to 86400s by default), so don't rely
  on sub-minute freshness.

## CI / release

- `.github/workflows/on-push.yml`: every push builds `ghcr.io/octopusden/octopus-teamcity-prometheus-exporter:<branch>`.
- `.github/workflows/on-release.yml`: publishing a GitHub release builds `:<tag>` and `:latest`.
- Release tags follow `release-X.Y.Z` (semver).
- Both workflows use the shared `octopusden/octopus-base` docker workflow.

## Conventions

- Branch names describe the change (e.g. `add-queue-metric`).
- Releases use semver: a new metric, field or env var is minor; a fix is patch; a rename or
  removal is major.

- Commit messages: one imperative sentence describing the behaviour change.
- This repository is public. Keep code, comments, commits and docs free of
  organisation-specific hostnames, project names and credentials.
