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

exec java ${JAVA_OPTS:-} --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED -cp /app.jar "$MAIN_CLASS"
