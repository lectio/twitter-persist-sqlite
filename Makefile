SHELL := /bin/bash
MAKEFLAGS := silent

# DCC_HOME is the devcontainer-common directory
DCC_HOME ?= /etc/devcontainer-common
MAKE_UTILS_HOME ?= $(DCC_HOME)/make

TTS_SOURCE_DB_FILE := ./tts-content.sqlite
CONTENT_PROCESSED_DB_FILE := ./links-content.sqlite
LINKS_CACHE_DB_FILE := ./links-cache.sqlite

$(TTS_SOURCE_DB_FILE):
	echo "Downloading $(TTS_SOURCE_DB_FILE) from https://github.com/medigy/digital-health-tweets-to-sqlite/content.sqlite.gz"
	curl --silent -Lo - "https://github.com/medigy/digital-health-tweets-to-sqlite/blob/master/content.sqlite.gz?raw=true" | gunzip > $(TTS_SOURCE_DB_FILE)

## Run the twitter to URL pipeline
pipeline: $(TTS_SOURCE_DB_FILE)
	python pipeline.py --config-url pipeline.conf.yaml

datasette:
	datasette serve --host 0.0.0.0 --port 8001 --immutable --immutable $(CONTENT_PROCESSED_DB_FILE)

## Reduce the size of SQLite databases by running OPTIMIZE for full text search tables, then VACUUM
compact:
	sqlite-utils optimize $(CONTENT_PROCESSED_DB_FILE)

## Remove all derived artifacts
clean:
	# Removing this will force the pipeline to run on the latest data
	rm -f $(TTS_SOURCE_DB_FILE)

ifneq ("$(wildcard $(MAKE_UTILS_HOME)/common.mk)","")
include $(MAKE_UTILS_HOME)/common.mk
else
$(info [WARN] common.mk was not found in $(MAKE_UTILS_HOME), missing some useful targets and utilities)
endif
