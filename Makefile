SHELL := /bin/bash
MAKEFLAGS := silent

TWITTER_AUTH_FILE := ./twitter-credentials.json

DB_HOME := db
TTS_SOURCE_DB_FILE := $(DB_HOME)/twitter-to-sqlite-unprocessed.sqlite
TTS_CRITERIA_DB_FILE := $(DB_HOME)/twitter-to-sqlite-criteria.sqlite
CONTENT_PROCESSED_DB_FILE := $(DB_HOME)/twitter-content-processed.sqlite
LINKS_CACHE_DB_FILE := $(DB_HOME)/links-cache.sqlite

DOC_SCHEMA_HOME := doc/schema
DOC_SCHEMA_CONTENT_HOME := $(DOC_SCHEMA_HOME)/$(TTS_SOURCE_DB_FILE)
DOC_SCHEMA_SOURCES_HOME := $(DOC_SCHEMA_HOME)/$(TTS_CRITERIA_DB_FILE)

$(TWITTER_AUTH_FILE):
	$(error $(NEWLINE)Twitter Credentials file $(YELLOW)$(TWITTER_AUTH_FILE)$(RESET) is missing.$(NEWLINE)Run $(GREEN)'make auth'$(RESET).$(NEWLINE)Use $(YELLOW)https://developer.twitter.com/en/apps$(RESET) to find your Twitter app credentials)

$(TTS_CRITERIA_DB_FILE):
	sqlitebiter -o $(TTS_CRITERIA_DB_FILE) file criteria/influencers.csv criteria/search-queries.csv

$(DOC_SCHEMA_CONTENT_HOME):
	java -jar /usr/local/bin/schemaspy.jar -t sqlite-xerial -db $(TTS_SOURCE_DB_FILE) -cat % -schemas "Content" -sso -dp /usr/local/bin/sqlite-jdbc.jar -o $(DOC_SCHEMA_CONTENT_HOME)

$(DOC_SCHEMA_SOURCES_HOME):
	java -jar /usr/local/bin/schemaspy.jar -t sqlite-xerial -db $(TTS_CRITERIA_DB_FILE) -cat % -schemas "Sources" -sso -dp /usr/local/bin/sqlite-jdbc.jar -o $(DOC_SCHEMA_SOURCES_HOME)

## Create the Twitter criteria database so it can be used by other targets
criteria: $(TTS_CRITERIA_DB_FILE)

## Prepare the Twitter Credentials file, required by the other targets
auth:
	twitter-to-sqlite auth --auth $(TWITTER_AUTH_FILE)

## Pull all the Tweets for the authorized user (the one whose credentials are being used)
user-timeline: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite user-timeline $(TTS_SOURCE_DB_FILE) --auth $(TWITTER_AUTH_FILE)

.ONESHELL:
## Using queries in the criteria database, run searches and populate Tweets in the content database
search: $(TWITTER_AUTH_FILE) $(TTS_CRITERIA_DB_FILE)
	sqlite3 $(TTS_CRITERIA_DB_FILE) "SELECT query FROM search_queries" | while read query
	do
		echo "Searching Twitter for '$$query' using twitter-to-sqlite, storing in $(TTS_SOURCE_DB_FILE)"
		twitter-to-sqlite search $(TTS_SOURCE_DB_FILE) "$$query" --auth $(TWITTER_AUTH_FILE) --since
	done

## Run the twitter to URL pipeline
pipeline:
	python pipeline.py --config-url pipeline.conf.yaml

## Reduce the size of SQLite databases by running OPTIMIZE for full text search tables, then VACUUM
compact:
	sqlite-utils optimize $(TTS_SOURCE_DB_FILE)
	sqlite-utils optimize $(CONTENT_PROCESSED_DB_FILE)
	sqlite-utils optimize $(LINKS_CACHE_DB_FILE)

## Create schema documentation for all the databases in this package
schema-doc: criteria $(DOC_SCHEMA_CONTENT_HOME) $(DOC_SCHEMA_SOURCES_HOME)

## Remove all derived artifacts
clean:
	rm -f $(TTS_CRITERIA_DB_FILE)
	rm -rf $(DOC_SCHEMA_HOME)

TARGET_MAX_CHAR_NUM=15
# All targets should have a ## Help text above the target and they'll be automatically collected
# Show help, using auto generator from https://gist.github.com/prwhite/8168133
help:
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${WHITE}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

comma := ,
define logInfo
	if [ "$(CCF_LOG_LEVEL)" = 'INFO' ]; then
		echo "$1"
	fi
endef

define NEWLINE


endef
