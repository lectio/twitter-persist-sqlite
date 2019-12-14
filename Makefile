SHELL := /bin/bash
MAKEFLAGS := silent

TWITTER_AUTH_FILE := ./twitter-credentials.json

DB_HOME := db
CONTENT_UNPROCESSED_DB_FILE := $(DB_HOME)/twitter-content-unprocessed.sqlite
SOURCES_DB_FILE := $(DB_HOME)/twitter-sources.sqlite
CONTENT_PROCESSED_DB_FILE := $(DB_HOME)/twitter-content-processed.sqlite

DOC_SCHEMA_HOME := doc/schema
DOC_SCHEMA_CONTENT_HOME := $(DOC_SCHEMA_HOME)/$(CONTENT_UNPROCESSED_DB_FILE)
DOC_SCHEMA_SOURCES_HOME := $(DOC_SCHEMA_HOME)/$(SOURCES_DB_FILE)

TWEETS_WITH_URLS_MDCONTENT_HOME := ./content/tweets

$(TWITTER_AUTH_FILE):
	echo "Twitter Credentials file $(TWITTER_AUTH_FILE) is missing. Run 'make auth'."
	echo "Use https://developer.twitter.com/en/apps to find your Twitter app credentials."
	echo ""
	exit

$(SOURCES_DB_FILE):
	sqlitebiter -o $(SOURCES_DB_FILE) file conf/influencers.csv conf/search-criteria.csv

$(DOC_SCHEMA_CONTENT_HOME):
	java -jar /usr/local/bin/schemaspy.jar -t sqlite-xerial -db $(CONTENT_UNPROCESSED_DB_FILE) -cat % -schemas "Content" -sso -dp /usr/local/bin/sqlite-jdbc.jar -o $(DOC_SCHEMA_CONTENT_HOME)

$(DOC_SCHEMA_SOURCES_HOME):
	java -jar /usr/local/bin/schemaspy.jar -t sqlite-xerial -db $(SOURCES_DB_FILE) -cat % -schemas "Sources" -sso -dp /usr/local/bin/sqlite-jdbc.jar -o $(DOC_SCHEMA_SOURCES_HOME)

## Create the Twitter sources database so it can be used by other targets
sources: $(SOURCES_DB_FILE)

## Prepare the Twitter Credentials file, required by the other targets
auth:
	twitter-to-sqlite auth --auth $(TWITTER_AUTH_FILE)

## Pull all the Tweets for the authorized user (the one whose credentials are being used)
user-timeline: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite user-timeline $(CONTENT_UNPROCESSED_DB_FILE) --auth $(TWITTER_AUTH_FILE)

## Using criteria in the sources database, run searches and populate Tweets in the content database
search: $(TWITTER_AUTH_FILE) $(SOURCES_DB_FILE)
	twitter-to-sqlite search $(CONTENT_UNPROCESSED_DB_FILE) "#HealthcareIT" --auth $(TWITTER_AUTH_FILE)

## Run the twitter to URL pipeline
pipeline:
	python pipeline.py \
		--content-unprocessed-db-url sqlite:///$(CONTENT_UNPROCESSED_DB_FILE) \
		--content-unprocessed-db-source-row-sql "select id, full_text from tweets" \
		--content-unprocessed-db-source-text-col-name "full_text" \
		--content-processed-db $(CONTENT_PROCESSED_DB_FILE) \
		--http-request-timeout-secs 5 \
		--config-url pipeline.conf.yaml

## Reduce the size of SQLite databases by running OPTIMIZE for full text search tables, then VACUUM
compact:
	sqlite-utils optimize $(CONTENT_UNPROCESSED_DB_FILE)
	sqlite-utils optimize $(CONTENT_PROCESSED_DB_FILE)

## Create schema documentation for all the databases in this package
schema-doc: sources $(DOC_SCHEMA_CONTENT_HOME) $(DOC_SCHEMA_SOURCES_HOME)

## Remove all derived artifacts
clean:
	rm -f $(SOURCES_DB_FILE)
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
