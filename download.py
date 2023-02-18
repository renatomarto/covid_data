import pandas as pd
import requests
import mysql.connector
import sys
from datetime import datetime, timedelta

# Verbinding maken met de database
try:
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="covid_data"
    )
except mysql.connector.Error as err:
    print(f"Er is een fout opgetreden bij het verbinden met de database: {err}")
    sys.exit()
 
url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
try:
    response = requests.get(url)
except requests.exceptions.RequestException as err:
    print(f"Er is een fout opgetreden bij het downloaden van het bestand: {err}")
    sys.exit()

# Lees de csv in als een pandas DataFrame
df = pd.read_csv(url)

# selecteer alleen de kolommen die u wilt behouden
columns_to_keep = ["iso_code", "continent", "location", "date", "total_cases", "new_cases", "new_cases_smoothed", "total_deaths", "new_deaths", "new_deaths_smoothed", "total_cases_per_million", "new_cases_per_million", "new_cases_smoothed_per_million", "total_deaths_per_million", "new_deaths_per_million", "new_deaths_smoothed_per_million", "reproduction_rate", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters", "new_vaccinations", "new_vaccinations_smoothed", "total_vaccinations_per_hundred", "people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred", "total_boosters_per_hundred", "new_vaccinations_smoothed_per_million", "new_people_vaccinated_smoothed", "new_people_vaccinated_smoothed_per_hundred", "stringency_index", "population_density", "median_age", "aged_65_older", "aged_70_older", "gdp_per_capita", "extreme_poverty", "cardiovasc_death_rate", "diabetes_prevalence", "female_smokers", "male_smokers", "handwashing_facilities", "hospital_beds_per_thousand", "life_expectancy", "human_development_index", "population", "excess_mortality_cumulative_absolute", "excess_mortality_cumulative", "excess_mortality", "excess_mortality_cumulative_per_million"]
df = df[columns_to_keep]

# filter alleen de rijen voor Suriname, Guyana, Frans-Guyana en Brazilië
countries_to_keep = ["SUR", "GUY", "GUF", "BRA"]
df = df[df["iso_code"].isin(countries_to_keep)]

# Vervang NaN-waarden in de kolommen "new_cases", "new_deaths" en "new_vaccinations" door 0
df = df.fillna(value={"new_cases": 0, "new_deaths": 0, "new_vaccinations": 0})

print("Database connection opened")

# Aanmaken van de tabel om de data op te slaan
mycursor = mydb.cursor()

table_name = "covid_data"

# Check if table exists
mycursor.execute(f"SHOW TABLES LIKE '{table_name}'")
result = mycursor.fetchone()

if result:
    # Truncate table if it exists
    mycursor.execute(f"TRUNCATE TABLE {table_name}")

create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, location VARCHAR(255), date DATE, new_cases INT, new_deaths INT, new_vaccinations INT)"
mycursor.execute(create_table_query)

# Voorbereiden van de data voor invoegen in de tabel
insert_query = f"INSERT INTO {table_name} (location, date, new_cases, new_deaths, new_vaccinations) VALUES (%s, %s, %s, %s, %s)"
data_to_insert = []

# Itereren over de data en alleen de data van de vier landen selecteren
for index, row in df.iterrows():
    if row["iso_code"] in countries_to_keep:
        data_to_insert.append((row["location"], row["date"], row["new_cases"], row["new_deaths"], row["new_vaccinations"] if not pd.isna(row["new_vaccinations"]) else 0))

# Invoegen van de data in de tabel
mycursor.executemany(insert_query, data_to_insert)

create_runtime_table_query = f"CREATE TABLE IF NOT EXISTS run_times (id INT AUTO_INCREMENT PRIMARY KEY,  run_time DATETIME )"
insert_runtime_query = f"INSERT INTO run_times (run_time) VALUES (NOW())"
mycursor.execute(create_runtime_table_query)
mycursor.execute(insert_runtime_query)


mydb.commit()

# Sluiten van de database connectie
mycursor.close()
mydb.close()

print("Database connection closed")
