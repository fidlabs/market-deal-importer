#!/bin/bash
docker rm -vf $(docker ps -aq)
cd /root/market-deal-importer 
docker compose up
