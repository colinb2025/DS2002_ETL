import requests
import mysql.connector
import csv
import mysql.connector

#Import statements to enable necessary libraries
#==========================================================================================================================================
#SQL Database connection

#Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Iswim4EST!",
    port="3306",
    database="ETL"
)

#Create a cursor object to enable SQL queries
cursor = db.cursor()

#SQL Database connection
#==========================================================================================================================================
#ETL SQL Database Creation

#Table queries for ETL database
create_country_table_query = """
CREATE TABLE IF NOT EXISTS Country (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    capital VARCHAR(255),  
    currencies VARCHAR(255),
    languages VARCHAR(255)
)
"""

create_capital_table_query = """
CREATE TABLE IF NOT EXISTS Capital (
    capital_id INT AUTO_INCREMENT PRIMARY KEY,
    country_id INT,
    country_name VARCHAR(255) NOT NULL,
    capital_name VARCHAR(255) NOT NULL
)
"""

create_currency_table_query = """
CREATE TABLE IF NOT EXISTS Currency (
    currency_id INT AUTO_INCREMENT PRIMARY KEY,
    country_id INT,
    name VARCHAR(255) NOT NULL,
    currency_name VARCHAR(50) NOT NULL
)
"""

create_language_table_query = """
CREATE TABLE IF NOT EXISTS Language (
    language_id INT AUTO_INCREMENT PRIMARY KEY,
    country_id INT,
    name VARCHAR(255) NOT NULL,
    language_name VARCHAR(50) NOT NULL
)
"""

create_country_language_table_query = """
CREATE TABLE IF NOT EXISTS FullName (
    country_id INT NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    language_name VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (country_id, full_name),
    UNIQUE (country_id, full_name, language_name),
    FOREIGN KEY (country_id) REFERENCES Country(country_id)
)
"""

#Table creation queries
try:
    cursor.execute(create_country_table_query)
    cursor.execute(create_capital_table_query)
    cursor.execute(create_currency_table_query)
    cursor.execute(create_language_table_query)
    cursor.execute(create_country_language_table_query)
except mysql.connector.Error as error:
    print("Error creating tables:", error)

#ETL SQL Database Creation
#==========================================================================================================================================
#API Data Importation

#Extracting data from the API and inserting it into the proper database within the ETL database
def extract_transform_load(country):
    try:
        endpoint = f"https://restcountries.com/v3.1/name/{country}?fullText=true"
        response = requests.get(endpoint)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and data[0].get("name"):
                country_data = data[0]
                full_name = country_data["name"].get("official", "")
                languages = ", ".join(country_data.get("languages", {}).keys())
                currencies = ", ".join(country_data.get("currencies", {}).keys())
                capital = ", ".join(country_data.get("capital", []))

                cursor.execute("INSERT INTO Country (name, capital, currencies, languages) VALUES (%s, %s, %s, %s)",
                            (full_name, capital, currencies, languages))
                country_id = cursor.lastrowid

                for currency in country_data.get("currencies", {}):
                    currency_name = country_data["currencies"][currency].get("name", "")
                    cursor.execute("INSERT INTO Currency (country_id, name, currency_name) VALUES (%s, %s, %s)",
                                (country_id, full_name, currency_name))

                for language in country_data.get("languages", {}):
                    language_name = country_data["languages"][language]
                    cursor.execute("INSERT INTO Language (country_id, name, language_name) VALUES (%s, %s, %s)",
                                (country_id, full_name, language_name))
                
                for capital_name in country_data.get("capital", []):
                    cursor.execute("INSERT INTO Capital (country_id, country_name, capital_name) VALUES (%s, %s, %s)",
                                   (country_id, full_name, capital_name))

                cursor.execute("INSERT INTO FullName (country_id, full_name) VALUES (%s, %s)",
                               (country_id, full_name))

                db.commit()
            else:
                print(f"No valid data found for {country}")
        else:
            print(f"Failed to retrieve data for {country}. Status code: {response.status_code}")
            print(f"Please make sure that '{country}' is your desired input")
    except mysql.connector.Error as sql_error:
        print("MySQL error:", sql_error)
    except requests.RequestException as req_error:
        print("Request error:", req_error)
    except Exception as e:
        print(f"An error occurred: {e}")

#Get a list of all current countries in the Country database
def get_country_list():
    try:
        cursor.execute("SELECT name FROM Country")
        country_list = [row[0] for row in cursor.fetchall()]
        return country_list
    except mysql.connector.Error as error:
        print("Error fetching country list:", error)
        return []

#Function to retrieve the official country name to assist with duplicates
def get_official_country_name(country):
    try:
        endpoint = f"https://restcountries.com/v3.1/name/{country}?fullText=true"
        response = requests.get(endpoint)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and data[0].get("name"):
                official_name = data[0]["name"].get("official", "")
                return official_name
            else:
                print(f"No valid data found for {country}")
        else:
            print(f"Failed to retrieve data for {country}. Status code: {response.status_code}")
            print(f"Please make sure that '{country}' is your desired input")
    except requests.RequestException as req_error:
        print("Request error:", req_error)
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

#Iteratively add countries to the Country database, accounting for duplicates 
countries = get_country_list()
added_countries = [] 
country = ""
while country is not None:
    country = input("What country would you like to add? If done, type 'done': ")
    if country.lower() == "done":
        print("")
        break
    else:
        official_name = get_official_country_name(country)
        if official_name:
            if official_name.lower() in [c.lower() for c in countries] or official_name.lower() in [c.lower() for c in added_countries]:
                print(f"{official_name} is already in the database.")
            else:
                added_countries.append(official_name)
                extract_transform_load(official_name)
                print(f"Data for {official_name} inserted successfully.")
        else:
            print(f"No official name found for {country}. Please try again.")

#API Data Importation
#==========================================================================================================================================
#CSV Data Importation

#Function to read the CSV into the ETL database
def read_csv(file_path):
    data = []
    with open(file_path, 'r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

#Loop to iteratively ask for a CSV file
while csv != None:
    csv_file_name = input("Name of your CSV file (no extension), 'done' if done: ")
    csv_file_path = f"{csv_file_name}.csv" 
    if csv_file_name.lower() == "done":
        print(f"")
        break
    else:
        try:
            csv_data = read_csv(csv_file_path)
            for row in csv_data:
                name = row['name']
                capital = row['capital']
                currencies = row['currencies']
                languages = row['languages']
                    
                cursor.execute("INSERT INTO Country (name, capital, currencies, languages) VALUES (%s, %s, %s, %s)",
                            (name, capital, currencies, languages))
                db.commit()
                print(f"Data for {name} inserted successfully.")
        except:
            print(f"{csv_file_path} couldn't be found, please try a different file")

#CSV Data Importation
#==========================================================================================================================================
#SQL Data Creation

#Creating the SQL table that we will import into our ETL database
create_table_query = """
CREATE TABLE IF NOT EXISTS SQLData (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    capital VARCHAR(255),
    currencies VARCHAR(255),
    languages VARCHAR(255)
)
"""

#Table creation query
try:
    cursor.execute(create_table_query)
except mysql.connector.Error as error:
    print("Error creating table:", error)

#Create data in SQL to import into our ETL database
SQL_data = [
    {
        "_id": "66133a9784c7fa0e6f2c869e",
        "name": "UVA",
        "capital": "Rotunda",
        "currencies": "Flex",
        "languages": "eng"
    },
    {
        "_id": "66133a9784c7fa0e6f2c869f",
        "name": "DS",
        "capital": "UVa",
        "currencies": "CAD",
        "languages": "python"
    },
    {
        "_id": "66133a9784c7fa0e6f2c86a0",
        "name": "Computer Science",
        "capital": "UVa",
        "currencies": "CAD",
        "languages": "java, python"
    }
]

#SQL Data Creation
#==========================================================================================================================================
#SQL Importation

# Insert data from SQLData into the Country table
for document in SQL_data:
    name = document.get("name")
    capital = document.get("capital")
    currencies = document.get("currencies")
    languages = document.get("languages")

    cursor.execute("SELECT name FROM Country WHERE name = %s", (name,))
    existing_country = cursor.fetchone()

    cursor.fetchall()

    if existing_country is None:
        cursor.execute("""
            INSERT INTO Country (name, capital, currencies, languages)
            VALUES (%s, %s, %s, %s)
        """, (name, capital, currencies, languages))
        print(f"Data for {name} inserted successfully into the Country table.")
    else:
        print(f"The country '{name}' already exists in the Country table. Skipping insertion.")

db.commit()

#SQL Importation
#==========================================================================================================================================
#Deletion Code

#Function for removing a country from the ETL database
def remove_country(table, country):
    try:
        if table == 'FullName':
            cursor.execute("DELETE FROM FullName WHERE country_id = (SELECT country_id FROM Country WHERE full_name = %s)", (country,))
        elif table == 'Country':
            cursor.execute("SELECT country_id FROM Country WHERE name = %s", (country,))
            country_id = cursor.fetchone()[0]
            cursor.execute("DELETE FROM FullName WHERE country_id = %s", (country_id,))
            cursor.execute("DELETE FROM Country WHERE country_id = %s", (country_id,))
        elif table == 'Capital':
            cursor.execute("DELETE FROM Capital WHERE country_name = %s", (country,))
        elif table == 'Currency':
            cursor.execute("DELETE FROM Currency WHERE country_id IN (SELECT country_id FROM Country WHERE name = %s)", (country,))
        elif table == 'Language':
            cursor.execute("DELETE FROM Language WHERE country_id IN (SELECT country_id FROM Country WHERE name = %s)", (country,))
        
        db.commit()
        print(f"{country} removed from {table} successfully.")
    except mysql.connector.Error as error:
        print(f"Error removing {country} from {table}: {error}")

#Loop to iteratively prompt the user to remove a country from a databse within the ETL database
modify_status = False
while not modify_status:
    modify = input("Would you like to modify your entries? Y/N: ").lower()
    modify = modify[0]
    if modify == "n":
        modify_status = True
        print(f"")
    elif modify == "y":
        table = input("What table would you like to modify? (Country, Capital, Currency, Language, FullName): ").capitalize()
        country = input("What country would you like to remove? ").capitalize()
        remove_country(table, country)
        more_modifications = input("Would you like to make more modifications? Y/N: ").lower()
        more_modifications = more_modifications[0]
        if more_modifications == "y":
            modify_status = False
        else:
            modify_status = True 
            print(f"")
    else:
        print("Please provide a valid input")
        modify_status = False

#Deletion code
#==========================================================================================================================================
#Statistics code

#Function to provide more prompts for the statistics request
def get_country_statistics():
    try:
        statistics = set()
        while True:
            choice = input("What statistic would you like from the Country table in SQL? (Name, Capital, Currencies, Languages): ").capitalize()
            if choice in ["Name", "Capital", "Currencies", "Languages"]:
                statistics.add(choice)
                more_statistics = input("Would you like to request more statistics? Y/N: ").lower()
                if more_statistics.startswith("n"):
                    break
            else:
                print("Invalid statistic. Please choose from the provided options.")
        return list(statistics)
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

#Function to retrieve the data from the Country database within the ETL database
def fetch_country_data(statistics):
    try:
        data = {}
        if "Name" in statistics:
            cursor.execute("SELECT DISTINCT name FROM Country")
            data["Name"] = [row[0] for row in cursor.fetchall()]
        if "Capital" in statistics:
            cursor.execute("SELECT DISTINCT capital FROM Country")
            data["Capital"] = [row[0] for row in cursor.fetchall()]
        if "Currencies" in statistics:
            cursor.execute("SELECT DISTINCT currencies FROM Country")
            currencies = [row[0] for row in cursor.fetchall()]
            currencies = [currency.split(", ") for currency in currencies]
            currencies = [currency for sublist in currencies for currency in sublist] 
            data["Currencies"] = list(set(currencies))  
        if "Languages" in statistics:
            cursor.execute("SELECT DISTINCT languages FROM Country")
            languages = [row[0] for row in cursor.fetchall()]
            languages = [language.split(", ") for language in languages]
            languages = [language for sublist in languages for language in sublist]  
            data["Languages"] = list(set(languages))  #
        return data
    except mysql.connector.Error as error:
        print(f"Error fetching country data: {error}")
        return {}

#Prompt the user to request Country statistics from ETL database
request_statistics = input("Would you like any statistics from the tables? Y/N: ").strip().lower()
if request_statistics.startswith("y"):
    requested_stats = get_country_statistics()
    if requested_stats:
        country_data = fetch_country_data(requested_stats)
        for stat, values in country_data.items():
            print(f"{stat}: {values}")
    else:
        print("No valid statistics could be found.")

# Close cursor and database connection
cursor.close()
db.close()
