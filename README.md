# Twitter Persistence for SQLite

Saves data from Twitter to a SQLite database using [dogsheep/twitter-to-sqlite](https://github.com/dogsheep/twitter-to-sqlite). 

## First time setup

* Run 'make auth' or supply a twitter-credentials.json file

## Routine execution

* Run 'make search' or 'make user-timeline' to grab the tweets and put them into a SQLite database

## Dev Container setup

* Uses default Python 3 configuration from Microsoft
* Uses Netspective default setup for zsh + Antigen + Oh My ZSH!
* Uses Netspective [Powerlevel9k](https://github.com/romkatv/powerlevel10k) ZSH theme

## Visual Studio setup

* Install [Meslo LG M Regular for Powerline.ttf](https://github.com/powerline/fonts/tree/master/Meslo%20Slashed) font for improved terminal typeface. This font is already set in devcontainer.json settings. 
* The default shell is /bin/zsh
