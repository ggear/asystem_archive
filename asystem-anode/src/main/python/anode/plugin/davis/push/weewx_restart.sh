#!/usr/bin/env bash

if ! service weewx status > /dev/null; then
    service weewx restart
fi
