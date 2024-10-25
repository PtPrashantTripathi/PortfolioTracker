#!/bin/bash

# Function to log messages with timestamps
log() {
  echo "$(date '+%H:%M:%S') - $1"
}

# Helper function to download and extract Spark
download() {
  local url=$1
  local dest=$2
  log "Downloading Spark binary from $url to $dest"
  curl -L $url -o spark.tgz || { log "Download failed"; exit 1; }
  tar -xzf spark.tgz -C "$dest" || { log "Extraction failed"; exit 1; }
  rm spark.tgz
}

# Input variables
SPARK_VERSION=${1:-"3.4.1"}
HADOOP_VERSION=${2:-"3"}
SCALA_VERSION=${3:-""}
PY4J_VERSION=${4:-"0.10.9.7"}
XMX=${5:-"4g"}
XMS=${6:-"1g"}
LOG_LEVEL=${7:-"WARN"}
SPARK_URL=${8:-""}

# Set the installation folder
WORKSPACE=${GITHUB_WORKSPACE:-"$HOME/spark-setup"}
mkdir -p "$WORKSPACE"
INSTALL_FOLDER="$WORKSPACE"
log "Spark will be installed to $INSTALL_FOLDER"

# Build the Spark binary path and download URL
SCALA_BIT=${SCALA_VERSION:+-scala$SCALA_VERSION}
SPARK_HOME="$INSTALL_FOLDER/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION$SCALA_BIT"

if [ -d "$SPARK_HOME" ]; then
  log "Using existing Spark installation at $SPARK_HOME"
else
  if [ -z "$SPARK_URL" ]; then
    SPARK_URL="https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION$SCALA_BIT.tgz"
    if ! curl --head --silent --fail "$SPARK_URL" > /dev/null; then
      log "Primary download URL not available, using Apache archive."
      SPARK_URL="https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION$SCALA_BIT.tgz"
    fi
  fi
  download "$SPARK_URL" "$INSTALL_FOLDER"
fi

# Verify Spark installation
if [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
  log "Error: Spark binary not found in $SPARK_HOME"
  exit 1
fi

# Set environment variables
log "Setting environment variables"
export SPARK_HOME=$SPARK_HOME
export HADOOP_VERSION=$HADOOP_VERSION
export APACHE_SPARK_VERSION=$SPARK_VERSION
export PYSPARK_PYTHON="python"
export PYSPARK_DRIVER_PYTHON="python"
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-$PY4J_VERSION-src.zip"
export SPARK_OPTS="--driver-java-options=-Xms$XMS --driver-java-options=-Xmx$XMX --driver-java-options=-Dlog4j.logLevel=$LOG_LEVEL"

# Add Spark to PATH
export PATH="$SPARK_HOME/bin:$PATH"

log "Spark setup complete. Version: $SPARK_VERSION, Hadoop: $HADOOP_VERSION"
