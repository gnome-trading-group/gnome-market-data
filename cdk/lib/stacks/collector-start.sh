#!/bin/sh
# ORCHESTRATOR_VERSION is pinned per collector by the create/redeploy Lambdas, so restarting a task downloads the
# same release again rather than drifting to a newer one.
set -eu
: "${ORCHESTRATOR_VERSION:?ORCHESTRATOR_VERSION must be set by the collector create/redeploy Lambda}"

GH_TOKEN=$(python3 - <<'PY'
import json

import boto3
from botocore.exceptions import ClientError


def fetch(region=None):
    return boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId="gnomepy/gh-token")["SecretString"]


# Falls back to us-east-1 when the secret is not replicated to the collector's region.
try:
    value = fetch()
except ClientError:
    value = fetch("us-east-1")
try:
    print(json.loads(value)["token"])
except (ValueError, KeyError, TypeError):
    print(value.strip())
PY
)

echo "start: fetching gnome-orchestrator ${ORCHESTRATOR_VERSION}"
curl -fsSL -u "gnome:${GH_TOKEN}" -o /app.jar \
  "https://maven.pkg.github.com/gnome-trading-group/gnome-orchestrator/group/gnometrading/gnome-orchestrator/${ORCHESTRATOR_VERSION}/gnome-orchestrator-${ORCHESTRATOR_VERSION}.jar"
unset GH_TOKEN

# A heap fixed at launch never grows, so it never needs the full collection that growing the JVM's near-empty
# default heap triggers: on a fractional vCPU that pause exhausts the CPU quota and stalls every socket read for up
# to a quarter second. 35% leaves the rest of the task's memory for the JIT, metaspace, threads and zstd buffers.
HEAP_OPTS="-XX:InitialRAMPercentage=35 -XX:MaxRAMPercentage=35 -XX:+AlwaysPreTouch"
exec java ${HEAP_OPTS} ${JAVA_OPTS:-} --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED -cp /app.jar "$MAIN_CLASS"
