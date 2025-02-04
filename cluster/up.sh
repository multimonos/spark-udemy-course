#!/usr/bin/env bash

if ps -ax |grep -v grep |grep nginx > /dev/null
then
  echo "Valet is running. killing it ..."
  valet stop
else
    echo "Valet is not running."
fi

# start containers
docker compose up -d

# check running container
docker compose ps

# open the ui
echo "Opening ui at http://localhost:8080"
sleep 3
open http://localhost:8080