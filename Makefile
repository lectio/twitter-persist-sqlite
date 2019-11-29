SHELL := /bin/bash
MAKEFLAGS := silent

TWITTER_AUTH_FILE := ./twitter-credentials.json
CONTENT_DB_FILE := ./twitter-content.sqlite.db

$(TWITTER_AUTH_FILE):
	echo "Twitter Credentials file $(TWITTER_AUTH_FILE) is missing. Run 'make auth'."
	exit

auth:
	twitter-to-sqlite auth --auth $(TWITTER_AUTH_FILE)

user-timeline: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite user-timeline $(CONTENT_DB_FILE) --auth $(TWITTER_AUTH_FILE)

search: $(TWITTER_AUTH_FILE)
	twitter-to-sqlite search $(CONTENT_DB_FILE) "#HealthcareIT" --auth $(TWITTER_AUTH_FILE)

compact:
	sqlite3 $(CONTENT_DB_FILE) "VACUUM;"

## See if all developer dependencies are installed
check-dependencies: check-t2s check-sqlite
	printf "[*] "
	make -v | head -1
	echo "[*] Shell: $$SHELL"

T2S_INSTALLED := $(shell command -v twitter-to-sqlite 2> /dev/null)
check-t2s:
ifndef T2S_INSTALLED
	echo "[ ] Did not find twitter-to-sqlite, run this to set it up:"
	echo "    pip install twitter-to-sqlite"
else
	printf "[*] "
	twitter-to-sqlite --version
endif

SQLITE_INSTALLED := $(shell command -v sqlite3 2> /dev/null)
check-sqlite:
ifndef SQLITE_INSTALLED
	echo "[ ] Did not find SQLite, install sqlite3 using package manager."
else
	printf "[*] SQLite "
	sqlite3 --version
endif

