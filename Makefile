SHELL := /bin/bash
MAKEFLAGS := silent

CURRENT_DIR_PATH := $(shell echo `pwd`)
CURRENT_DIR_NAME := $(shell basename `pwd`)

TWITTER_AUTH_FILE := ./twitter-credentials.json
CONTENT_DB_FILE := ./twitter-content.db.sqlite

T2S_INSTALLED := $(shell command -v twitter-to-sqlite 2> /dev/null)
check-t2s:
ifndef T2S_INSTALLED
	pip install twitter-to-sqlite
endif

$(TWITTER_AUTH_FILE): check-t2s
	echo "Twitter Credentials file $(TWITTER_AUTH_FILE) is missing. Run 'make auth'."
	exit

auth: check-t2s
	twitter-to-sqlite auth --auth $(TWITTER_AUTH_FILE)

user-timeline: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite user-timeline $(CONTENT_DB_FILE) --auth $(TWITTER_AUTH_FILE)

search: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite search $(CONTENT_DB_FILE) "#HealthcareIT" --auth $(TWITTER_AUTH_FILE)

