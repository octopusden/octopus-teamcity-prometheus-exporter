# TeamCity Prometheus Exporter

Turns the state of a TeamCity server into Prometheus metrics, so build health can be
graphed and alerted on.

## TeamCity

**Build configuration**:
One TeamCity job definition. Every metric about builds is about one build configuration.
_Avoid_: build type (the API's name for it), job, config (alone, in prose)

**Template**:
A TeamCity template that build configurations inherit from. Only the status and duration
metrics are limited to the watched templates. The JDK and total-configuration metrics cover
a whole project subtree, and failed builds cover a project subtree selected by monitored
steps, whatever the template.
_Avoid_: base configuration, parent

**Meta-runner**:
A reusable build step type shared by many build configurations. A failed build is labelled
with the meta-runner step that failed.
_Avoid_: recipe (TeamCity's newer name for the same thing; use it only when naming that API),
plugin

**Project chain**:
The upstream dependency builds of a successful build of a stop template. Its duration runs
from the start of the first upstream build that belongs to a start template, in the order
TeamCity lists them (not necessarily the earliest start), to the finish of the stop-template
build. It is reported under the stop-template build's project.
_Avoid_: pipeline, build chain (alone)

## What the exporter reports

**Failed build**:
A build configuration and branch pair that failed within the time window, whose latest
build on that branch still has status FAILURE, at a monitored meta-runner step or monitored
step name. It stops being a failed build as soon as the latest build has any other status.
If TeamCity can't answer which build is latest or which step failed, the pair is kept as a
failed build labelled `<attribution-error>`, without a confirmed monitored-step failure.
_Avoid_: red build, broken build, failing config

**Excluded subtree**:
A TeamCity project and all its subprojects that the exporter is told to ignore.
_Avoid_: blacklist, ignored projects

**Archived project**:
A project TeamCity has archived. Its build configurations are skipped like an excluded
subtree, but the exporter finds them by itself.
_Avoid_: inactive project, deleted project

**Known-answer probe**:
A question asked of TeamCity whose correct answer is known to be non-empty. Each correct
answer is counted, so a counter that stops moving means no metric from this exporter can be
trusted, even while the process is up.
_Avoid_: health check, liveness probe, ping

## How it updates

**Loop**:
One of the exporter's independent update cycles (full metrics, status, failed builds,
known-answer probe). Each collects, updates its metrics as it goes, then sleeps for its own
interval.
_Avoid_: scrape (that is Prometheus reading the exporter), job, thread

**Update path**:
The route by which one metric gets its values: which loop sets it, what that loop filters,
and when the metric is cleared. Two metrics on different update paths can disagree for a
while.
_Avoid_: pipeline, flow
