SHELL := /bin/bash
MAKEFLAGS := silent

CURRENT_DIR_PATH := $(shell echo `pwd`)
CURRENT_DIR_NAME := $(shell basename `pwd`)

TWITTER_AUTH_FILE := ./twitter-credentials.json
CONTENT_DB_FILE := ./twitter-content.db.sqlite

$(TWITTER_AUTH_FILE):
	echo "Twitter Credentials file $() is missing. Run 'make auth'."
	exit

auth:
	twitter-to-sqlite auth --auth $(TWITTER_AUTH_FILE)

user-timeline: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite user-timeline $(CONTENT_DB_FILE) --auth $(TWITTER_AUTH_FILE)

