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
cd $DIR && docker build -t rxs2367/daprd:1.0.0 .
docker push rxs2367/daprd:1.0.0