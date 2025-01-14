#!/bin/bash
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
DIR=$(dirname "$SCRIPT")
# echo $SCRIPTPATH
BUILDPATH="$(dirname "$DIR")"
FILE=${DIR}"/Dockerfile"
echo $BUILDPATH
echo $FILE
# echo $FILEPATH
cd $DIR && docker build -t rxs2367/daprd:2.0.0 .
docker push rxs2367/daprd:2.0.0

make clean

####### Dapr build commands ##########
export DAPR_REGISTRY=docker.io/rxs2367
export DAPR_TAG=dev
make build-linux
make docker-build
docker tag rxs2367/daprd:dev-linux-amd64 rxs2367/daprd:dev-linux-amd64_bt2_monitoring6
docker push rxs2367/daprd:dev-linux-amd64_bt2_monitoring6

