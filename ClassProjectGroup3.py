# course: cmps3500
# CLASS Project
# PYTHON IMPLEMENTATION: BASIC DATA ANALYSIS
# Date: 04/02/24
# Student 1: Jacob Acosta
# Student 2: Jason Alvarez
# Student 3: Fang Lin
# Student 4: John Pocasangre
# description: Implementation Basic Data Analysys Routines

import pandas as pd
from datetime import datetime

def load_data(file_path):

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # List of columns to keep (replace this with your specific column names)
    columns_to_keep = ['ID', 'Severity', 'Start_Time', 'End_Time', 'Distance(mi)',
                       'Description', 'City', 'State', 'Zipcode', 'Country',
                       'Timezone', 'Weather_Timestamp', 'Temperature(F)', 
                        'Humidity(%)', 'Pressure(in)', 'Visibility(mi)',
                        'Precipitation(in)', 'Weather_Condition']

    # Keep only the specified columns in the DataFrame
    df_filtered = df.loc[:, columns_to_keep]

    return df_filtered

def process_data(df):
    
    start_time = datetime.now()

    # Perform data cleaning tasks
    # 1 Eliminate rows with missing data in specified columns
    required_columns = ['ID', 'Severity', 'Zipcode', 'Start_Time', 'End_Time', 
                        'Visibility(mi)', 'Weather_Condition', 'Country']
    
    df_cleaned = df.dropna(subset=required_columns)

    # 2 Eliminate rows with empty values in 3 or more columns
    df_cleaned = df_cleaned.dropna(thresh=len(required_columns) - 3)

    # 3 Eliminate rows with distance equal to zero
    df_cleaned = df_cleaned[df_cleaned['Distance(mi)'] != 0]

    # 4 Consider only the first 5 digits of the zip code
    df_cleaned['Zipcode'] = df_cleaned['Zipcode'].astype(str).str[:5]

    # 5 Eliminate rows with zero time duration
    df_cleaned['Start_Time'] = pd.to_datetime(df_cleaned['Start_Time'])
    df_cleaned['End_Time'] = pd.to_datetime(df_cleaned['End_Time'])
    df_cleaned = df_cleaned[df_cleaned['End_Time'] - df_cleaned['Start_Time'] != pd.Timedelta(0)]

    return df_cleaned, start_time

def print_answers():
    print("Printing answers...")
    # Add your code to print answers here

def search_accidents_by_location():
    city = input("Enter city: ")
    state = input("Enter state: ")
    zip_code = input("Enter zip code: ")
    print(f"Searching accidents in {city}, {state}, {zip_code}...")
    # Add your code to search accidents by location here

def search_accidents_by_date():
    year = input("Enter year: ")
    month = input("Enter month: ")
    day = input("Enter day: ")
    print(f"Searching accidents on {year}-{month}-{day}...")
    # Add your code to search accidents by date here

def search_accidents_by_conditions():
    min_temp = input("Enter minimum temperature: ")
    max_temp = input("Enter maximum temperature: ")
    visibility = input("Enter visibility range: ")
    print(f"Searching accidents with temperature between {min_temp} and {max_temp} and visibility {visibility}...")
    # Add your code to search accidents by conditions here

def quit_program():
    print("Quitting program...")
    quit()

###########################################################################
#        Main Logic
###########################################################################

# actual path to the CSV file
file_path = 'US_Accidents_data.csv'
#file_path = 'Boston_Lyft_Uber_Data.csv'
#file_path = 'InputDataSample.csv'

# define Dataframe
data={}
df_filtered = pd.DataFrame(data)

while True:
    print("(1) Load data\n"
      "(2) Process data\n"
      "(3) Print Answers\n"
      "(4) Search Accidents (Use City, State, and Zip Code)\n"
      "(5) Search Accidents (Year, Month and Day)\n"
      "(6) Search Accidents (Temperature Range and Visibility Range)\n"
      "(7) Quit")

    choice = input("Enter your choice: ")

    if choice == '1':
        
        start_time = datetime.now()
        df_filtered = load_data(file_path)
        
        # Get the current time after loading the CSV
        current_time = datetime.now()
        print("Loading and cleaning input data set:")
        print("*******************************************")
        print(f"[{current_time}] Starting Script")
        print(f"[{current_time}] Loading {file_path}")

        # Get the total number of columns and rows in the DataFrame
        total_columns = len(df_filtered.columns)
        total_rows = len(df_filtered)

        print(f"[{current_time}] Total Columns Read: {total_columns}")
        print(f"[{current_time}] Total Rows Read: {total_rows}")

        # Calculate and print the time taken to load the data
        time_to_load = (current_time - start_time).total_seconds()
        print(f"\nTime to load is: {time_to_load} seconds.")
              
    elif choice == '2':
        
        if len(df_filtered) == 0:
            print ("Please load data firstly")
        else:

            df_cleaned, current_time = process_data(df_filtered)

            # Print the cleaned DataFrame and its dimensions
            print("\nProcessing input data set:")
            print("**********************************")

            print(df_cleaned.head())
            print(f"\nDimensions after cleaning: {df_cleaned.shape}")

            # Calculate and print the time taken to clean the data
            cleaning_time = (datetime.now() - current_time).total_seconds()
            print(f"\nTime to clean data is: {cleaning_time} seconds.")

    elif choice == '3':
        print_answers()
    elif choice == '4':
        search_accidents_by_location()
    elif choice == '5':
        search_accidents_by_date()
    elif choice == '6':
        search_accidents_by_conditions()
    elif choice == '7':
        quit_program()
    else:
        print("Invalid choice. Please enter a number from 1 to 7.")

