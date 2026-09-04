#!/bin/sh
set -eu

curl --request POST http://localhost:8080/events \
  --header 'Content-Type: application/json' \
  --data '{"event_id":"evt-release-42","kind":"release.published","project":"compiler","revision":"a1b2c3d","status":"succeeded","webhook_url":"http://localhost:9090/hooks/builds"}'
