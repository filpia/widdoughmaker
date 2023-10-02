#!/usr/bin/env bash
set -euo pipefail

TAG=$1

# build local image
docker build . -t widdoughmaker:$TAG

# tag local image
docker tag widdoughmaker:$TAG 400419513456.dkr.ecr.us-east-1.amazonaws.com/widdoughmaker:$TAG

# push local image
docker push 400419513456.dkr.ecr.us-east-1.amazonaws.com/widdoughmaker:$TAG