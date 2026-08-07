FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless wget \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=65534:65534 app.jar /app.jar

RUN printf '%s\n' \
    '#!/bin/sh' \
    'exec java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED -cp /app.jar "$MAIN_CLASS"' \
    > /start.sh \
    && chmod 755 /start.sh

USER 65534:65534
ENTRYPOINT ["/start.sh"]
