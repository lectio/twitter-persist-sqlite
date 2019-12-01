# Twitter Persistence for SQLite

Saves data from Twitter to a SQLite database using [dogsheep/twitter-to-sqlite](https://github.com/dogsheep/twitter-to-sqlite). 

## First time setup

Twitter credentials are required, which may be obtained at https://developer.twitter.com/en/apps. 

* Run 'make auth' or supply a twitter-credentials.json file

## Routine execution

* Run 'make search' or 'make user-timeline' to grab the tweets and put them into a SQLite database

## Dev Container setup

* Uses default Python 3 configuration from Microsoft
* Installs Netspective default setup for zsh + Antigen + Oh My ZSH!
* Installs [Powerlevel9k](https://github.com/romkatv/powerlevel10k) ZSH theme through Antigen
* Installs SQLite 3.x from Debian
* Installs Latest JDK from Debian
* Installs [SchemaSpy](http://schemaspy.org/) database documentation tool from GitHub
* Installs Xerial SQLite JDBC Driver from BitBucket

## Visual Studio setup

* Install [Meslo LG M Regular for Powerline.ttf](https://github.com/powerline/fonts/tree/master/Meslo%20Slashed) font for improved terminal typeface. This font is already set in devcontainer.json settings. 
* The default shell is /bin/zsh

# TODO

* Integrate [SQLite Web](https://github.com/coleifer/sqlite-web) database management utility to allow editing and viewing of databases.