#!/usr/bin/env bash

odir=./logs

ts=$(date "+%Y%m%d%H%M%S")

shuf -n $((RANDOM % 50 + 1)) ./access_log.txt > "${odir}/${ts}.txt"

ls -l $odir/*.txt

