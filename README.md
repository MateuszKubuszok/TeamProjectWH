TeamProject WareHouse
====

Project of a Data WareHouse for TeamProject.

More of a proof of concept than an actual WareHouse.

Prerequisites
----

WareHouse requires JRuby installed as well as components needed to run required libraries:
PostgreSQL. On Debian those libraries can be installed with `sudo apt-get install libmysqlclient-dev libpq-dev`.

JRuby itself can be installed via rvm. Gems used by project can be installed with bundler:
call `bundle install` from root directory of a checke out project. If bundler is not available
install it with `gem install bundle`.

Installation
----

Since databases used by WareHouse are created and populated with TeamProject no further configuration is required.
 
Starting up server
----

Server can be started up with `rails s` command run in a project's root.

