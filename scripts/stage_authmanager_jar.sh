#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AUTHMANAGER_DIR="$ROOT_DIR/spark/authmanager-prototype"
SPARK_DOCKER_DIR="$ROOT_DIR/spark/docker"
JUPYTER_DOCKER_DIR="$ROOT_DIR/jupyterhub-docker"

cd "$ROOT_DIR"

echo "[authmanager-stage] Building AuthManager jar..."
mvn -f "$AUTHMANAGER_DIR/pom.xml" -DskipTests package >/dev/null

JAR_PATH="$(find "$AUTHMANAGER_DIR/target" -maxdepth 1 -type f -name 'teehr-iceberg-authmanager-prototype-*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | head -n1)"

if [[ -z "${JAR_PATH:-}" || ! -f "$JAR_PATH" ]]; then
  echo "[authmanager-stage] Failed to find built jar under $AUTHMANAGER_DIR/target" >&2
  exit 1
fi

echo "[authmanager-stage] Staging jar from $JAR_PATH"
cp "$JAR_PATH" "$SPARK_DOCKER_DIR/teehr-authmanager.jar"
cp "$JAR_PATH" "$JUPYTER_DOCKER_DIR/teehr-authmanager.jar"

echo "[authmanager-stage] Staged jar to:"
echo "  - $SPARK_DOCKER_DIR/teehr-authmanager.jar"
echo "  - $JUPYTER_DOCKER_DIR/teehr-authmanager.jar"
