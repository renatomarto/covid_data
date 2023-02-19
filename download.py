import pandas as pd
import requests
import mysql.connector
import sys
import socket
import time



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


# Vraag de naam van de gebruiker
name = ""
while not name:
    name = input("Wat is je naam? ")

# Vraag om bevestiging om door te gaan
confirmation = input(f"Dag {name}, wil je het script uitvoeren? (ja/nee) ")
if confirmation.lower() != "ja":
    print("Het script wordt niet uitgevoerd.")
    sys.exit()

# Aantal seconden dat het script bezig is
totaal_seconden = 10

# Loop voor de duur van het script
for i in range(totaal_seconden):
    # Wacht één seconde voordat we verdergaan
    time.sleep(1)
    # Print een puntje op één lijn zonder newline
    print(".", end="", flush=True)

print("\nData wordt ingelezen")
url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
try:
    response = requests.get(url)
except requests.exceptions.RequestException as err:
    print(f"Er is een fout opgetreden bij het downloaden van het bestand: {err}")
    sys.exit()

# Lees de csv in als een pandas DataFrame
df = pd.read_csv(url)


# selecteer alleen de kolommen die u wilt behouden
print("Data wordt gefilterd op kolommen")
columns_to_keep = ["iso_code", "location", "date", "new_cases", "new_deaths","new_vaccinations","total_tests"]
df = df[columns_to_keep]

# filter alleen de rijen voor Suriname, Guyana, Frans-Guyana en Brazilië
print("Data wordt gefilterd op land")
countries_to_keep = ["SUR", "GUY", "GUF", "BRA"]
df = df[df["iso_code"].isin(countries_to_keep)]

# Vervang NaN-waarden in de kolommen "new_cases", "new_deaths" en "new_vaccinations" door 0
df = df.fillna(value={"new_cases": 0, "new_deaths": 0, "new_vaccinations": 0, "total_tests": 0})

print("Database connection opened")

# Aanmaken van de tabel om de data op te slaanprint a loading period in python
mycursor = mydb.cursor()
print("Database wordt gecreerd")
table_name = "covid_data"
mycursor.execute(f"DROP TABLE IF EXISTS {table_name}")
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, iso_code VARCHAR(255), location VARCHAR(255), date DATE, new_cases INT, new_deaths INT, new_vaccinations INT, total_tests INT)"
mycursor.execute(create_table_query)

# Voorbereiden van de data voor invoegen in de tabel
print("Database wordt ingevoerd")
insert_query = f"INSERT INTO {table_name} (iso_code,location, date, new_cases, new_deaths, new_vaccinations, total_tests) VALUES (%s, %s, %s, %s, %s, %s, %s)"
data_to_insert = []

# Itereren over de data en alleen de data van de vier landen selecteren
for index, row in df.iterrows():
    if row["iso_code"] in countries_to_keep:
        data_to_insert.append((row["iso_code"] ,row["location"], row["date"], row["new_cases"], row["new_deaths"], row["new_vaccinations"] ,(row["total_tests"])))
    

# Invoegen van de data in de tabel
mycursor.executemany(insert_query, data_to_insert)

ip_address = socket.gethostbyname(socket.gethostname())

print("Audit trail geupdate")
create_audit_trail_table_query = f"CREATE TABLE IF NOT EXISTS audit_trail (id INT AUTO_INCREMENT PRIMARY KEY,  run_time DATETIME, name  VARCHAR(255), ip_address  VARCHAR(255) )"
insert_audit_trail_query = f"INSERT INTO audit_trail (run_time , name,ip_address) VALUES (NOW(), '{name}', '{ip_address}')"
mycursor.execute(create_audit_trail_table_query)
mycursor.execute(insert_audit_trail_query)

mydb.commit()

# Sluiten van de database connectie
mycursor.close()
mydb.close()
print("Database connection closed")
print("Script succesvol gerund! Have a nice day!")
