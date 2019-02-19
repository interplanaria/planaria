#!/bin/bash

set -e
if [[ "$1" == "start" ]]; then

  echo "# Starting Mongodb......."
  echo "arg0: $2"
  echo "arg1: $3"
  echo "arg2: $4"
  echo "arg3: $5"
  sudo mongod --bind_ip_all --wiredTigerCacheSizeGB=$2 &
  until nc -z localhost 27017
    do
      sleep 1
    done

  echo "# Inheriting package.json...."
  node /planaria/merge /planaria
  echo "# npm install..."
  npm install

  echo "# Starting Planaria......."
  node --max-old-space-size=4096 /planaria/index $3 $4 $5
fi

exec "$@"
