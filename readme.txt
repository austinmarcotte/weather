Weather App
Austin Marcotte
marcotteaustin@gmail.com

##########
# Set Up #
##########

1. Create a new postgres database, then run ddl.sql to create the tables needed for this script.
2. Install the modules (all available via cpan) at the top of weather.pl


#######
# Use #
#######

Script requires 3 arguments:

perl weather.pl command latitude longitude

Available commands:
- scrape	Will only scrape the data for that location
- display	Will display the values in the database for noon for all upcoming days
- both		Will scrape the data and then display the results for noon for all upcoming days
- display-full	Will display the values in the database for all upcoming hours


#########
# About #
#########

Script will scrape from three sources:

1. National Weather Service (only just realized this only works for U.S. coordinates, but the script will catch this and continue)
2. Visual Crossing
3. Meteo

NWS and Meteo will pull in data via JSON while Visual Crossing will use the CSV format.


As standard ETL process, the data will initially be loaded into a staging table and then aggregated into the final table that display will pull from. In this case the staging data will only overwrite the existing data if there are the same number of sources or more in the latest scrape. The database is designed so more sources can easily be added to the code.
