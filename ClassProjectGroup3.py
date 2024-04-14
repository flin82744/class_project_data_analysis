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
import numpy as np
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
    df_cleaned = df_cleaned.dropna(thresh=len(df_cleaned.columns) - 3)

    # 3 Eliminate rows with distance equal to zero
    df_cleaned = df_cleaned[df_cleaned['Distance(mi)'] != 0]

    # 4 Consider only the first 5 digits of the zip code
    df_cleaned['Zipcode'] = df_cleaned['Zipcode'].astype(str).str[:5]

    # 5 Eliminate rows with zero time duration
    df_cleaned['Start_Time'] = pd.to_datetime(df_cleaned['Start_Time'])
    df_cleaned['End_Time'] = pd.to_datetime(df_cleaned['End_Time'])
    df_cleaned = df_cleaned[df_cleaned['End_Time'] - df_cleaned['Start_Time'] != pd.Timedelta(0)]

    return df_cleaned, start_time

def q1_answer(df_cleaned):
    df_cleaned.loc[:, 'Year_Month'] = pd.to_datetime(df_cleaned['Start_Time']).dt.strftime('%Y-%m')
    df_count_by_column = df_cleaned.groupby(['Year_Month'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    
    return df_count_by_column

def q2_answer(df_cleaned):
    df_cleaned.loc[:, 'Year'] = pd.to_datetime(df_cleaned['Start_Time']).dt.strftime('%Y')
    df_count_by_column = df_cleaned.groupby(['Year'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)

    return df_count_by_column

def q3_answer(df_cleaned):
    df_cleaned = df_cleaned[df_cleaned['Severity'] == 2]
    df_count_by_column = df_cleaned.groupby(['State'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    state_most = df_count_by_column.head(1)
    query_state = str(state_most['State'].values[0])
    df_cleaned = df_cleaned[df_cleaned['State'] == query_state]
    df_cleaned.loc[:, 'Year'] = pd.to_datetime(df_cleaned['Start_Time']).dt.strftime('%Y')
    df_count_by_column = df_cleaned.groupby(['Year'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)

    return state_most, df_count_by_column

def q4_answer(df_cleaned, input_state):
    df_cleaned_state = df_cleaned[df_cleaned['State'] == input_state]

    # Count the occurrences of each value in the 'Category' column
    value_counts = df_cleaned_state.groupby(['Severity'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    # Get the maximum occurrence
    max_occurrence = value_counts.head(1)

    return max_occurrence

def q5_answer(df_cleaned):
    df_cleaned_CA = df_cleaned[df_cleaned['State'] == 'CA']
    
    # Count the occurrences of each value in the 'Category' column
    value_counts_CA = df_cleaned_CA.groupby(['City'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    # Get the maximum five occurrence
    max_five_df_CA = value_counts_CA.head(5)

    return df_cleaned_CA, max_five_df_CA

def q5_answer_display_year(df_cleaned_CA, max_five_df_CA, row, col):

    # The city of the most accident
    first_city_value = str(max_five_df_CA.iloc[row,col])
    first_city_df = df_cleaned_CA[df_cleaned_CA['City'] == first_city_value] 
    first_city_df = first_city_df.copy()
    first_city_df['Year'] = first_city_df['Start_Time'].dt.year
    first_df_by_city = first_city_df.groupby(['Year'])['End_Time'].count().reset_index(name='Count').sort_values(    ['Count'], ascending=False)

    return first_city_value, first_df_by_city

def q6_answer(df_cleaned,severity, city):
    df_cleaned_county = df_cleaned[df_cleaned['City'] == city]
    df_cleaned_severity = df_cleaned_county[df_cleaned_county['Severity'] == severity]
    df_cleaned_q6 = df_cleaned_severity.dropna()
    
    # Extract month from 'Start_Time' and create a new column
    df_cleaned_q6 = df_cleaned_q6.copy()
    df_cleaned_q6['Month'] = df_cleaned_q6['Start_Time'].dt.month_name()

    # Calculate the average humidity and temperature per month
    avg_by_month = df_cleaned_q6.groupby('Month')[['Humidity(%)', 'Temperature(F)']].mean()
    
    return avg_by_month

def q7_answer(df_cleaned, city):
    # Filter the DataFrame by state
    df_state = df_cleaned[df_cleaned['City'] == city]

    # Extract month from 'Start_Time' and create a new column
    df_state = df_state.copy()
    df_state['Month'] = df_state['Start_Time'].dt.month_name()

    # Get the three most common weather conditions per month
    most_common_conditions = df_state.groupby(['Month', 'Weather_Condition'])['ID'].count().reset_index(name='AccidentCount')
    most_common_conditions = most_common_conditions.sort_values(['Month', 'AccidentCount'], ascending=[True, False])
    most_common_conditions = most_common_conditions.groupby('Month').head(3)

    return most_common_conditions

def q8_answer(df_cleaned, state, severity):
    # Filter the DataFrame by severity and state
    df_filtered = df_cleaned[(df_cleaned['Severity'] == severity) & (df_cleaned['State'] == state)]

    # Calculate the maximum visibility among severity 2 accidents
    max_visibility = df_filtered['Visibility(mi)'].max()

    return max_visibility

def q9_answer(df_cleaned, city):
    # Filter the DataFrame by city
    df_city = df_cleaned[df_cleaned['City'] == city]

    # Extract year from 'Start_Time' and create a new column
    df_city = df_city.copy()
    df_city['Year'] = df_city['Start_Time'].dt.year

    # Count accidents by severity and year
    accidents_by_severity_year = df_city.groupby(['Year', 'Severity'])['ID'].count().reset_index(name='AccidentCount')

    return accidents_by_severity_year

def q10_answer(df_cleaned, location, months):
    # Filter the DataFrame by location and months
    df_location_months = df_cleaned[(df_cleaned['City'] == location) & (df_cleaned['Start_Time'].dt.month.isin(months))]

    # Calculate accident duration in hours
    df_location_months = df_location_months.copy()
    df_location_months['Accident_Duration'] = (df_location_months['End_Time'] - df_location_months['Start_Time']).dt.total_seconds() / 3600

    # Find the longest accident duration per year
    longest_accident = df_location_months.groupby([df_location_months['Start_Time'].dt.year])['Accident_Duration'].max()

    return longest_accident

def search_accidents_by_location(df_cleaned, state=None, city=None, zip_code=None):
    city = str(input("Enter city: "))
    state = str(input("Enter state: "))
    zip_code = input("Enter zip code: ")
    filters = []
    if state:
        filters.append(df_cleaned['State'] == state)
    if city:
        filters.append(df_cleaned['City'] == city)
    if zip_code:
        filters.append(df_cleaned['Zipcode'] == zip_code)
    
    if filters:
        filtered_df = df_cleaned[pd.concat(filters, axis=1).all(axis=1)]
    else:
        filtered_df = df_cleaned
    accident_count = len(filtered_df)

    return accident_count


def search_accidents_by_date(df_cleaned, year=None, month=None, day=None):
    year = input("Enter year: ")
    month = input("Enter month: ")
    day = input("Enter day: ")

    filters = []
    
    if year:
        filters.append(df_cleaned['Start_Time'].dt.year.astype(str) == year)
    if month:
        filters.append(df_cleaned['Start_Time'].dt.month.astype(str) == month)
    if day:
        filters.append(df_cleaned['Start_Time'].dt.day.astype(str) == day)
    
    if filters:
        filtered_df = df_cleaned[pd.concat(filters, axis=1).all(axis=1)]
    else:
        filtered_df = df_cleaned

    accident_count = len(filtered_df)

    return accident_count

def search_accidents_by_conditions(df_cleaned, temp_min=None, temp_max=None, vis_min=None, vis_max=None):
    temp_min = float(input("Enter minimum temperature: "))
    temp_max = float(input("Enter maximum temperature: "))
    vis_min = float(input("Enter minimum visibility: "))
    vis_max = float(input("Enter maximum visibility: "))

    filters = []
    if temp_min is not None:
        filters.append(df_cleaned['Temperature(F)'] >= temp_min)
    if temp_max is not None:
        filters.append(df_cleaned['Temperature(F)'] <= temp_max)
    if vis_min is not None:
        filters.append(df_cleaned['Visibility(mi)'] >= vis_min)
    if vis_max is not None:
        filters.append(df_cleaned['Visibility(mi)'] <= vis_max)
    
    if filters:
        filtered_df = df_cleaned[pd.concat(filters, axis=1).all(axis=1)]
    else:
        filtered_df = df_cleaned  # No filters applied, use the entire DataFrame
    
    # Count the number of accidents after filtering
    accident_count = len(filtered_df)

    return accident_count

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
df_cleaned = pd.DataFrame(data)

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
        print("\nLoading and cleaning input data set:")
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
        print(f"\nTime to load is: {time_to_load} seconds.\n")
              
    elif choice == '2':
        
        if len(df_filtered) == 0:
            print ("\nPlease load data firstly!\n")
        else:

            df_cleaned, current_time = process_data(df_filtered)

            # Print the cleaned DataFrame and its dimensions
            print("\nProcessing input data set:")
            print("**********************************")
            print(f"[{current_time}] Performing Data Clean Up")
            total_rows = len(df_cleaned)
            print(f"[{current_time}] Total Rows Read after cleaning is:{total_rows}")
            # Calculate and print the time taken to clean the data
            cleaning_time = (datetime.now() - current_time).total_seconds()
            print(f"\nTime to process is: {cleaning_time} seconds.\n")

    elif choice == '3':
        if len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print("\nAnswering questions:")
            print("**********************************")
            
            print("\nWhat are the 3 months with the highest amount accidents reported?")
            df_q1 = q1_answer(df_cleaned)      
            print(df_q1.head(3))

            print("\nWhat is the year with the highest amount of accidents reported?")
            df_q2 = q2_answer(df_cleaned)
            print(df_q2.head(1))

            print("\nWhat is the state that had the most accidents of severity 2? Display the data per year.")
            df_q3_1, df_q3_2 = q3_answer(df_cleaned)
            print(df_q3_1)
            print(f'\n{df_q3_2}')

            print("\nWhat severity is the most common in Virginia, California and Florida?")
            df_q4_1 = q4_answer(df_cleaned, 'VA')
            df_q4_2 = q4_answer(df_cleaned, 'CA')
            df_q4_3 = q4_answer(df_cleaned, 'FL')
            print(f"In Virginia: \n{df_q4_1}\n")
            print(f"In California: \n{df_q4_2}\n")
            print(f"In Florida: \n{df_q4_3}\n")

            print("\nWhat are the 5 cities that had the most accidents in in California?Display the data per year.")
            df_q5_CA, df_q5_max_five = q5_answer(df_cleaned)
            df_q5_1, df_q5_2 = q5_answer_display_year(df_q5_CA, df_q5_max_five, 0, 0)
            print(f"In California: \n{df_q5_max_five}\n")
            print(f"The first city is {df_q5_1}: \n{df_q5_2}\n")
            df_q5_3, df_q5_4 = q5_answer_display_year(df_q5_CA, df_q5_max_five, 1, 0)
            print(f"The second city is {df_q5_3}: \n{df_q5_4}\n")
            df_q5_5, df_q5_6 = q5_answer_display_year(df_q5_CA, df_q5_max_five, 2, 0)
            print(f"The third city is {df_q5_5}: \n{df_q5_6}\n")
            df_q5_7, df_q5_8 = q5_answer_display_year(df_q5_CA, df_q5_max_five, 3, 0)
            print(f"The fourth city is {df_q5_7}: \n{df_q5_8}\n")
            df_q5_9, df_q5_10 = q5_answer_display_year(df_q5_CA, df_q5_max_five, 4, 0)
            print(f"The fifth city is {df_q5_9}: \n{df_q5_10}\n")

            print("What was the average humidity and average temperature of all accidents of severity 4 that occurred in Boston City? display the data per month.\n")
            df_q6 = q6_answer(df_cleaned, 4, 'Boston')
            print(f"The average of humidity and temerature: \n{df_q6}\n")

            print("What are the 3 most common weather conditions (weather_conditions) when accidents occurred in New York? display the data per month.\n")
            df_q7 = q7_answer(df_cleaned, 'New York')
            print(f"The 3 most common in New York: \n{df_q7}\n")

            print("What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?\n")
            df_q8 = q8_answer(df_cleaned, 'NH', 2)
            print(f"The maximum visibility in New Hsmpshire: \n{df_q8}\n")

            print("How many accidents of each severity were recorded in Bakersfield? Display the data per year.\n")
            df_q9 = q9_answer(df_cleaned, 'Bakersfield')
            print(f"The accidents' number of each severity level in Bakersfield: \n{df_q9}\n")

            print("What was the longest accident (in hours) recorded in Las Vegas in the Spring (March, April, and May)? Display the data per year.\n")
            location_name = 'Las Vegas'
            spring_months = [3, 4, 5]
            df_q10 = q10_answer(df_cleaned, location_name, spring_months)
            print(f"The longest accident (in hours) recorded in Las Vegas in the Spring: \n{df_q10}\n")

    elif choice == '4':
        if len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print("Search Accidents: ")
            print("********************************************")
            res = search_accidents_by_location(df_cleaned)
            print(f"There were {res} accidents.\n")

    elif choice == '5':
        if len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print("Search Accidents: ")
            print("********************************************")
            res = search_accidents_by_date(df_cleaned)
            print(f"There were {res} accidents.\n")

    elif choice == '6':
        if len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print("Search Accidents: ")
            print("********************************************")
            res = search_accidents_by_conditions(df_cleaned)
            print(f"There were {res} accidents.\n")
    
    elif choice == '7':
        quit_program()
    
    else:
        print("Invalid choice. Please enter a number from 1 to 7.")
