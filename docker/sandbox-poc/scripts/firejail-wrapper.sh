#!/bin/bash
# Firejail wrapper for source-declarative-manifest
firejail --noprofile --quiet --private -- python /airbyte/integration_code/main.py "$@"
