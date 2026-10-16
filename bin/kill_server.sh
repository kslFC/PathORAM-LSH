#!/bin/sh
ps aux | grep java | grep com.server.Server | kill `awk '{print $2}'`
