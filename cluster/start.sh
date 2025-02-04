#!/usr/bin/env bash

# start containers
docker compose up -d

# check running container
docker compose ps

# more details
#docker compose ps -a

# view logs
docker compose logs -f

# view logs for service named 'web'
# docker compose logs -f web