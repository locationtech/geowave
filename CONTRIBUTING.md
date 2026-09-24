Before your contribution can be accepted by the project, you need to create an Eclipse Foundation 
account and electronically sign the Eclipse Contributor Agreement (ECA).

- https://www.eclipse.org/legal/eca/

For more information on contributing to GeoWave, please see our developer guide here:

- https://locationtech.github.io/geowave/devguide.html#how-to-contribute

## Third-party dependencies

GeoWave is an Eclipse Foundation project, so every dependency it redistributes
has to have its licence accounted for. The [Eclipse Dash license
tool](https://github.com/eclipse-dash/dash-licenses) checks this, and the
`IP Check` workflow runs it on every pull request.

To reproduce that check locally:

```
./mvnw -B -DskipTests install
./.utility/dash-summary.sh /tmp/DEPENDENCIES.generated
./.utility/dash-gate.sh DEPENDENCIES /tmp/DEPENDENCIES.generated
```

`install` is needed first because the summary would otherwise be missing
GeoWave's own inter-module dependencies.

The check fails when a change introduces content that is not already in the
committed `DEPENDENCIES` baseline. It deliberately does not fail on the
existing backlog of content awaiting review — the point is to stop that backlog
growing while it is worked down.

If you have added something that trips the check, the options are, in order of
preference: use a dependency that is already approved, drop the dependency, or
ask a committer to file a review request with the Eclipse IP team. Every push
to master files requests for all restricted content, skipping anything that
already has one. Committers can also file by running the `IP Check` workflow
manually with `file_reviews` enabled. Both need a `gitlab.eclipse.org` personal
access token with the `api` scope stored as the `DASH_IPLAB_TOKEN` repository
secret.

A manual run can be limited: `include_group_ids` restricts it to the given
Maven group IDs, and `include_artifact_ids` narrows it further where one group
mixes current and outgoing versions.

When a change removes restricted content, the check lists it as resolved.
Replace `DEPENDENCIES` with the `DEPENDENCIES` artifact from that run so the
baseline keeps shrinking.
