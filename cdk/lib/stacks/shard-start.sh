#!/bin/sh
# ORCHESTRATOR_VERSION is pinned per shard by the reconciler (its task definition), so a restarted task downloads the
# same release again rather than drifting to a newer one; a new version reaches shards only through a roll.
set -eu
: "${ORCHESTRATOR_VERSION:?ORCHESTRATOR_VERSION must be set in the shard task definition}"

GH_TOKEN=$(python3 - <<'PY'
import json

import boto3
from botocore.exceptions import ClientError


def fetch(region=None):
    return boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId="gnomepy/gh-token")["SecretString"]


# Falls back to us-east-1 when the secret is not replicated to the shard's region.
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
# ZGC collects concurrently, so a collection doesn't stop the socket readers the way a G1 pause does; one
# concurrent thread keeps it from taking the whole CPU quota. Minute files are kept in direct memory, outside this
# heap. Setting GC_OPTS=-XX:+UseG1GC falls back to G1; adding it to JAVA_OPTS instead would select two collectors,
# which the JVM refuses to start with.
GC_OPTS="${GC_OPTS:--XX:+UseZGC -XX:ConcGCThreads=1}"
exec java ${HEAP_OPTS} ${GC_OPTS} ${JAVA_OPTS:-} --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED -cp /app.jar group.gnometrading.orchestrator.Main shard
