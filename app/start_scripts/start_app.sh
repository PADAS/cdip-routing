#!/bin/bash

faust -A app.subscribers.kafka_subscriber worker -l info --without-web
