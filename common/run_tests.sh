#!/bin/sh
echo Testing common
cd /app
APP_ENV=circleci python -m unittest discover -s /app/common -v -p '*test*.py'

