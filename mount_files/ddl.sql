/*
Name: Fernaldy Aristo Wirjowerdojo

// ddl.sql //
This SQL file was created to be executed as an entrypoint for the postgres container that:
1. Creates the table called `table_sdg`
2. Populate the table using the raw data obtained from Kaggle

NOTE: There is no need to create a database as part of this SQL file because it is already created 
when the container is run as specified by the `.env` file 
*/

BEGIN;

-- Create table
CREATE TABLE table_sdg (
    "Entity" VARCHAR(255),
    "Year" INT,
    "Access to electricity (% of population)" FLOAT,
    "Access to clean fuels for cooking" FLOAT,
    "Renewable-electricity-generating-capacity-per-capita" FLOAT,
    "Financial flows to developing countries (US $)" FLOAT,
    "Renewable energy share in the total final energy consumption (%)" FLOAT,
    "Electricity from fossil fuels (TWh)" FLOAT,
    "Electricity from nuclear (TWh)" FLOAT,
    "Electricity from renewables (TWh)" FLOAT,
    "Low-carbon electricity (% electricity)" FLOAT,
    "Primary energy consumption per capita (kWh/person)" FLOAT,
    "Energy intensity level of primary energy (MJ/$2017 PPP GDP)" FLOAT,
    "Value_co2_emissions_kt_by_country" FLOAT,
    "Renewables (% equivalent primary energy)" FLOAT,
    "gdp_growth" FLOAT,
    "gdp_per_capita" FLOAT,
    "Density\n(P/Km2)" VARCHAR(255),
    "Land Area(Km2)" FLOAT,
    "Latitude" FLOAT,
    "Longitude" FLOAT
);

COMMIT;


BEGIN;

-- Import the csv file into the table
COPY table_m3 
FROM '/files/data_raw.csv'
DELIMITER ','
CSV HEADER;

COMMIT;
