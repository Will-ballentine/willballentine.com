#!/bin/sh
# Build for the main site (willballentine.com Worker). Cloudflare runs: sh build-main.sh
rm -rf dist && mkdir -p dist && cp -r index.html css toads assets dist/
