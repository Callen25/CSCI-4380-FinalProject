# Readme

## Summary

This application is designed to provide easy exploration of NFL game data from 2002-2013, with the additional context of how the weather affected those games.

The data used can be found in the `standings.csv` and `weather_20131231.csv` files.


## Setup

In order to use this application, a `dbms_final_project` database must be made, and `dbms_project_user` with password `dbms_password` must be created. You can run the code in `db-setup.sql` to do this.

Then the data needs to be loaded by running the `load_data.py` file.

Once this is complete the `application.py` file can be run from the command line.

Example setup code:
psql -U postgres postgres < db-setup.sql
python load_data.py
python application.py


## Application Use

Once the application is running, the user will be prompted to input which NFL team they would like to learn about, and then what option they want to learn from there. The team input can accept any substring of the full name, as long as it is enough to determine the team uniquely.


The options on what you can learn about a team are as follows:

Weather Statistics - How well the team did in windy, hot, and cold games.<br>
Playoff History<br>
Most Difficult Season<br>
Season Statistics - Record of the team for a season, or all available seasons.<br>
Super Bowls - List of Super Bowl appearances

