# course: cmps3500
# CLASS Project
# PYTHON IMPLEMENTATION: BASIC DATA ANALYSIS
# Date: 04/02/24
# Student 1: Jacob Acosta
# Student 2: Jason Alvarez
# Student 3: Fang Lin
# Student 4: John Pocasangre
# description: Implementation Basic Data Analysys Routines

import os
import pandas as pd
from datetime import datetime

#################################################
# Load Data, Process Data, And Integration
# By Fang Lin
################################################

# Scan file from directory
# Input: the path
# Output: an array of the file name

def read_file_from_directory(directory_path):
    try:
        files_list = os.listdir(directory_path)
        csv_files = [file for file in files_list if file.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("No CSV files found in the directory.")

    except FileNotFoundError as e: 
        print(f"Error: {e}\n")
        return None
    return csv_files

# Load Data Function:
# Input: an csv file path
# Output: a data frame with 18 specific column 

def load_data(file_path):

    # Read the CSV file into a pandas DataFrame
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return None
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

    # List of columns to keep
    columns_to_keep = ['ID', 'Severity', 'Start_Time', 'End_Time', 'Distance(mi)',
                       'Description', 'City', 'State', 'Zipcode', 'Country',
                       'Timezone', 'Weather_Timestamp', 'Temperature(F)',
                        'Humidity(%)', 'Pressure(in)', 'Visibility(mi)',
                        'Precipitation(in)', 'Weather_Condition']

    try:
        # Keep only the specified columns in the DataFrame
        df_filtered = df.loc[:, columns_to_keep]
    except KeyError:
        print ("Columns to keep do not exists, Please check it")
        return None

    return df_filtered

# Process Data Function:
# Input: a data frame
# Output: a clean data frame

def process_data(df):

    if df is None:
        print("No data to process. Please load data first.")
        return None, None

    # Perform data cleaning tasks
    # 1 Eliminate rows with missing data in specified columns
    required_columns = ['ID', 'Severity', 'Zipcode', 'Start_Time', 'End_Time',
                        'Visibility(mi)', 'Weather_Condition', 'Country']

    try:
        df_cleaned = df.dropna(subset=required_columns)

        # Eliminate rows with empty values in 3 or more columns
        df_cleaned = df_cleaned.dropna(thresh=len(df_cleaned.columns) - 3)

        # Eliminate rows with distance equal to zero
        df_cleaned = df_cleaned[df_cleaned['Distance(mi)'] != 0]

        # Consider only the first 5 digits of the zip code
        df_cleaned['Zipcode'] = df_cleaned['Zipcode'].astype(str).str[:5]

        # Eliminate rows with zero time duration
        df_cleaned['Start_Time'] = pd.to_datetime(df_cleaned['Start_Time'], format='mixed')
        df_cleaned['End_Time'] = pd.to_datetime(df_cleaned['End_Time'], format='mixed')
        df_cleaned = df_cleaned[df_cleaned['End_Time'] - df_cleaned['Start_Time'] != pd.Timedelta(0)]

    except Exception as e:
        print(f"An error occurred during data processing: {e}")
        return None
    return df_cleaned

#################################################
# Questions From 1 to 5
# By Jason Alvarez
################################################

def q1_answer(df_cleaned):
    # df_clean is properly cleaned
    if df_cleaned is None or len(df_cleaned) == 0:
        return None

    df_cleaned.loc[:, 'Year_Month'] = pd.to_datetime(df_cleaned['Start_Time']).dt.strftime('%Y-%m')
    df_count_by_column = df_cleaned.groupby(['Year_Month'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)

    return df_count_by_column


def q2_answer(df_cleaned):
    if df_cleaned is None or len(df_cleaned) == 0:
        return None

    df_cleaned.loc[:, 'Year'] = pd.to_datetime(df_cleaned['Start_Time']).dt.strftime('%Y')
    df_count_by_column = df_cleaned.groupby(['Year'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)

    return df_count_by_column


def q3_answer(df_cleaned, severity_level):
    if df_cleaned is None or len(df_cleaned) == 0:
        return None, None

    df_filtered = df_cleaned[df_cleaned['Severity'] == severity_level]
    df_count_by_column = df_filtered.groupby(['State'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    state_most = df_count_by_column.head(1)
    query_state = str(state_most['State'].values[0])
    df_filtered_state = df_filtered[df_filtered['State'] == query_state]
    df_filtered_state = df_filtered_state.copy()
    df_filtered_state.loc[:, 'Year'] = pd.to_datetime(df_filtered_state['Start_Time']).dt.strftime('%Y')
    df_count_by_column = df_filtered_state.groupby(['Year'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)

    return state_most, df_count_by_column

def q4_answer(df_cleaned, state):
    if df_cleaned is None or len(df_cleaned) == 0:
        return None

    df_filtered_state = df_cleaned[df_cleaned['State'] == state]

    # Count the occurrences of each severity level
    value_counts = df_filtered_state.groupby(['Severity'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    # Get the maximum occurrence
    max_occurrence = value_counts.head(1)

    return max_occurrence

def q5_answer(df_cleaned):
    if df_cleaned is None or len(df_cleaned) == 0:
        return None, None

    df_cleaned_CA = df_cleaned[df_cleaned['State'] == 'CA']

    # Count the occurrences of accidents in each city in California
    value_counts_CA = df_cleaned_CA.groupby(['City'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    # Get the five cities with the highest occurrence
    max_five_df_CA = value_counts_CA.head(5)

    return df_cleaned_CA, max_five_df_CA

def q5_answer_display_year(df_cleaned_CA, max_five_df_CA):
    if df_cleaned_CA is None or max_five_df_CA is None:
        return None, None

    results = []
    for index, row in max_five_df_CA.iterrows():
        city = row['City']
        city_df = df_cleaned_CA[df_cleaned_CA['City'] == city]
        city_df = city_df.copy()
        city_df['Year'] = city_df['Start_Time'].dt.year
        df_by_city_year = city_df.groupby(['Year'])['End_Time'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
        results.append((city, df_by_city_year))

    return results

#################################################
# Questions From 6 to 10
# By Jacob Acosta
################################################

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
    most_common_conditions = df_state.groupby(['Weather_Condition'])['ID'].count().reset_index(name='AccidentCount')
    most_common_conditions = most_common_conditions.sort_values(['AccidentCount'], ascending=False)
    most_common_conditions = most_common_conditions.head(3)

    return df_state, most_common_conditions

def q7_answer_display_month(df_state, most_common_conditions, row, col):

    # The conditions of the most accident
    first_value = str(most_common_conditions.iloc[row,col])
    first_df = df_state[df_state['Weather_Condition'] == first_value]
    first_df = first_df.copy()
    first_df['Month'] = first_df['Start_Time'].dt.month
    first_df_by_month = first_df.groupby(['Month'])['ID'].count().reset_index(name='AccidentCount')
    first_df_by_month = first_df_by_month.sort_values(['Month'])

    first_df_by_month.reset_index(drop=True, inplace=True)
    first_df_by_month.index += 1

    return first_value, first_df_by_month

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

    longest_accident.index = range(1, len(longest_accident) + 1)

    return longest_accident


#################################################
# Search Capability
# By John Pocasangre
################################################


def search_accidents_by_location(df_cleaned):
    #making sure that the data isn't empty, cuz if it is we will print our statement
    #and return an empty dataframe to prevent an error 
    if df_cleaned.empty:
        print("The dataset is empty. No data to search.")
        return 0, pd.DataFrame()  

    #turning user inputs into lowercase
    city = input("Enter city: ").strip().lower()
    state = input("Enter state: ").strip().lower()
    zip_code = input("Enter zip code: ").strip()
    filters = []
    
    #added filters, and made it take both lowercase and uppercase states and cities
    if state:
        if not state.isalpha():
            print("Invalid input for state. Please enter only letters.")
        else:
            filters.append(df_cleaned['State'].str.lower() == state)
    if city:
        if not city.replace(" ", "").isalpha():
            print("Invalid input for city. Please enter only letters.")
        else:
            filters.append(df_cleaned['City'].str.lower() == city)
    if zip_code:
        if not zip_code.isdigit():
            print("Invalid input for zip code. Please enter only numbers.")
        else:
            filters.append(df_cleaned['Zipcode'] == zip_code)

    #filters and error catching
    if filters:
        try:
            mask = filters[0]
            for f in filters[1:]:
                mask &= f
            filtered_df = df_cleaned[mask]
        except Exception as e:
            print(f"An error occurred while filtering the data: {e}")
            return 0, pd.DataFrame()
    else:
        filtered_df = df_cleaned

    #then printing out he amounts of accidents in that city/state/areacode
    accident_count = len(filtered_df)
    if accident_count == 0:
        print("No accidents found for the given criteria.")
    return accident_count, filtered_df

def search_accidents_by_date(df_cleaned):
    #like in the previous S1, we check if the data frame is empty
    if df_cleaned.empty:
        print("The dataset is empty. No data to search.")
        return 0

    
    valid_filters = True  
    #ask the user if the year, month and day they want to enter
    year = input("Enter year: ").strip()
    month = input("Enter month: ").strip()
    day = input("Enter day: ").strip()
    filters = []

    try:
        #check if the input and filter are valid 
        if year:
            if not year.isdigit() or len(year) != 4:
                print("Invalid input for year. Please enter a four-digit year.")
                valid_filters = False  
            else:
                filters.append(df_cleaned['Start_Time'].dt.year == int(year))

        #check if month input and filter are also valid
        if month:
            if not month.isdigit() or not 1 <= int(month) <= 12:
                print("Invalid input for month. Please enter a month number between 1 and 12.")
                valid_filters = False  
            else:
                filters.append(df_cleaned['Start_Time'].dt.month == int(month))

        #same as previous, this time just for day
        if day:
            if not day.isdigit() or not 1 <= int(day) <= 31:
                print("Invalid input for day. Please enter a day number between 1 and 31.")
                valid_filters = False 
            else:
                filters.append(df_cleaned['Start_Time'].dt.day == int(day))

        #keep checking if the filters are valid
        if filters and valid_filters:
            mask = filters[0]
            for f in filters[1:]:
                mask &= f 
            filtered_df = df_cleaned[mask]
        else:
            #print, if there are any errors 
            if not valid_filters:
                print("Please correct the errors and try again.")
                return 0  
            filtered_df = pd.DataFrame() 

        # we print out our number of accidents 
        accident_count = len(filtered_df)
        if accident_count == 0:
            print("No accidents found for the specified date.")
        return accident_count
    except Exception as e:
        #and throw out an error if any happened during the process
        print(f"An error occurred during the search: {e}")
        return 0


def search_accidents_by_conditions(df_cleaned):
    try:
        temp_min_input = input("Enter minimum temperature: ")
        temp_max_input = input("Enter maximum temperature: ")
        vis_min_input = input("Enter minimum visibility: ")
        vis_max_input = input("Enter maximum visibility: ")

        filters = []

        if temp_min_input:
            temp_min = float(temp_min_input)
            filters.append(df_cleaned['Temperature(F)'] >= temp_min)
        if temp_max_input:
            temp_max = float(temp_max_input)
            if temp_min_input and temp_min > temp_max:
                print("Minimum tempereature cannot be greater than maximum temperature .")
                return 0
            filters.append(df_cleaned['Temperature(F)'] <= temp_max)
        if vis_min_input:
            vis_min = float(vis_min_input)
            filters.append(df_cleaned['Visibility(mi)'] >= vis_min)
        if vis_max_input:
            vis_max = float(vis_max_input)
            if vis_min_input and vis_min > vis_max:
                print("Minimum visibility cannot be greater than maximum visibility.")
                return 0
            filters.append(df_cleaned['Visibility(mi)'] <= vis_max)
        if filters:
            mask = filters[0]
            for f in filters[1:]:
                mask &= f
            filtered_df = df_cleaned[mask]
        else:
            filtered_df = df_cleaned

        accident_count = len(filtered_df)
        return accident_count

    except ValueError:
        print("Invalid input. Please enter a valid number.")
        return 0

def quit_program():
    print("Quitting program...")
    quit()

###########################################################################
#        Test Functions
###########################################################################

class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\33[0m'

# Test all
def test_all():
    pass_test = False
    if test_load_data():
        pass_test = True
        print(Color.GREEN + "\n------load_data() test1 passed test!\n" + Color.RESET)
    else:
        print(Color.RED+"------load_data() test1 not passed test!\n"+Color.RESET)

    if test_process_data():
        pass_test = True
        print(Color.GREEN + "------process_data() test2 passed test!\n" + Color.RESET)
    else:
        print(Color.RED+"------process_data() test2 not passed test!\n"+Color.RESET)

    return pass_test

# Test load_data function: load an error file and get None 
def test_load_data():
    pass_load = False
    test_df = load_data(test_file_path)

    if (test_df) == None:
        pass_load = True

    return pass_load

# Test process_data function: to compare the results
# between reading file and load data
def test_process_data():
    pass_process = False
    test_df =  pd.read_csv(file_path)
    test_df_rows = len(test_df)
    test_df_columns = len(test_df.columns)

    loaded_df = load_data(file_path)
    loaded_df_columns = len(loaded_df.columns)

    if (test_df_columns != loaded_df_columns):
        pass_process = True

    processed_df = process_data(loaded_df)
    processed_df_rows = len(processed_df)

    if (test_df_rows != processed_df_rows):
        pass_process = True

    return pass_process

###########################################################################
#        Main Logic
###########################################################################

# actual path to the CSV file
file_path = 'US_Accidents_data.csv'
test_file_path = 'Boston_Lyft_Uber_Data.csv'
#file_path = 'InputDataSample.csv'

# define Dataframe
data={}
df_filtered = pd.DataFrame(data)
df_cleaned = pd.DataFrame(data)
total_time = 0.0

# unit test
#pass_test = test_all()
#if not pass_test:
#    print("\nPlease check the functions that are not passed")

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
        
        # read files from directory
        directory_path = './'
        csv_files = read_file_from_directory(directory_path)
        
        if csv_files is None:
            break
        
        print(Color.GREEN)
        print("\nCSV Files in the Directory, enter # to choose:")
        for index, csv_file in enumerate(csv_files):
            print(f"{index}: {csv_file}")

        start_time = datetime.now()
        input_index = input("\nLoading and cleaning input data set:")
       
        confirm_file_path = None
        try:
            new_index = int(input_index)
            if 0 <= new_index < len(csv_files):
                chosen_csv_file = csv_files[new_index]
                confirm_file_path = os.path.join(directory_path, chosen_csv_file)
                print(f"file path: {confirm_file_path}")
            else:
                print("Invalid index entered.")
        except ValueError:
            print("Invalid input. Please enter a valid integer index.")

        df_filtered = None
        if (confirm_file_path is not None):
            df_filtered = load_data(confirm_file_path)
        # Get the current time after loading the CSV
        current_time = datetime.now()

        if (df_filtered is not None):
            print("*******************************************")
            print(f"[{current_time}] Starting Script")
            print(f"[{current_time}] Loading {confirm_file_path}")

            # Get the total number of columns and rows in the DataFrame
            total_columns = len(df_filtered.columns)
            total_rows = len(df_filtered)

            print(f"[{current_time}] Total Columns Read: {total_columns}")
            print(f"[{current_time}] Total Rows Read: {total_rows}")

            # Calculate and print the time taken to load the data
            time_to_load = (current_time - start_time).total_seconds()
            total_time += (time_to_load/60)
            print(f"\nTime to load is: {time_to_load} seconds.\n")
        print(Color.RESET)

    elif choice == '2':

        if df_filtered is None or len(df_filtered) == 0:
            print ("\nPlease load data firstly!\n")
        else:

            start_time = datetime.now()
            df_cleaned = process_data(df_filtered)
            if df_cleaned is not None:
                current_time = datetime.now()
                # Print the cleaned DataFrame and its dimensions
                print(Color.GREEN)
                print("\nProcessing input data set:")
                print("**********************************")
                print(f"[{start_time}] Performing Data Clean Up")
                total_rows = len(df_cleaned)
                print(f"[{current_time}] Total Rows Read after cleaning is:{total_rows}")
                # Calculate and print the time taken to clean the data
                cleaning_time = (current_time - start_time).total_seconds()
                total_time += cleaning_time/60
                print(f"\nTime to process is: {cleaning_time} seconds.\n")
                print(Color.RESET)

    elif choice == '3':
        if df_cleaned is None or len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print(Color.GREEN)
            print("\nAnswering questions:")
            print("**********************************")

            start_time1 = datetime.now()
            print(f"\n[{start_time1}] What are the 3 months with the highest amount accidents reported?")
            df_q1 = q1_answer(df_cleaned)
            print(df_q1.head(3))

            start_time2 = datetime.now()
            print(f"\n[{start_time2}] What is the year with the highest amount of accidents reported?")
            df_q2 = q2_answer(df_cleaned)
            print(df_q2.head(1))

            start_time3 = datetime.now()
            print(f"\n[{start_time3}] What is the state that had the most accidents of severity 2? Display the data per year.")
            df_q3_1, df_q3_2 = q3_answer(df_cleaned, 2)
            print(df_q3_1)
            print(f'\n{df_q3_2}')

            start_time4 = datetime.now()
            print(f"\n[{start_time4}] What severity is the most common in Virginia, California and Florida?")
            df_q4_1 = q4_answer(df_cleaned, 'VA')
            df_q4_2 = q4_answer(df_cleaned, 'CA')
            df_q4_3 = q4_answer(df_cleaned, 'FL')
            print(f"In Virginia: \n{df_q4_1}\n")
            print(f"In California: \n{df_q4_2}\n")
            print(f"In Florida: \n{df_q4_3}\n")

            start_time5 = datetime.now()
            print(f"\n[{start_time5}] What are the 5 cities that had the most accidents in in California?Display the data per year.")
            df_q5_CA, df_q5_max_five = q5_answer(df_cleaned)
            df_q5_1 = q5_answer_display_year(df_q5_CA, df_q5_max_five)
            print(f"In California: \n{df_q5_max_five}\n")
            print(f"The result is: \n{df_q5_1}\n")

            start_time6 = datetime.now()
            print(f"\n[{start_time6}] What was the average humidity and average temperature of all accidents of severity 4 that occurred in Boston City? display the data per month.\n")
            df_q6 = q6_answer(df_cleaned, 4, 'Boston')
            print(f"The average of humidity and temerature: \n{df_q6}\n")

            start_time7 = datetime.now()
            print(f"\n[{start_time7}] What are the 3 most common weather conditions (weather_conditions) when accidents occurred in New York? display the data per month.\n")
            df_q7_NY, df_q7_most_three = q7_answer(df_cleaned, 'New York')
            df_q7_1, df_q7_2 = q7_answer_display_month(df_q7_NY, df_q7_most_three, 0, 0)
            print(f"The 3 most common in New York: \n{df_q7_most_three}\n")
            print(f"The first common is {df_q7_1}: \n{df_q7_2}\n")
            df_q7_3, df_q7_4 = q7_answer_display_month(df_q7_NY, df_q7_most_three, 1, 0)
            print(f"The second common is {df_q7_3}: \n{df_q7_4}\n")
            df_q7_5, df_q7_6 = q7_answer_display_month(df_q7_NY, df_q7_most_three, 2, 0)
            print(f"The third common is {df_q7_5}: \n{df_q7_6}\n")

            start_time8 = datetime.now()
            print(f"\n[{start_time8}] What was the maximum visibility of all accidents of severity 2 that occurred in the state of New Hampshire?\n")
            df_q8 = q8_answer(df_cleaned, 'NH', 2)
            print(f"The maximum visibility in New Hsmpshire: \n{df_q8}\n")

            start_time9 = datetime.now()
            print(f"\n[{start_time9}] How many accidents of each severity were recorded in Bakersfield? Display the data per year.\n")
            df_q9 = q9_answer(df_cleaned, 'Bakersfield')
            print(f"The accidents' number of each severity level in Bakersfield: \n{df_q9}\n")

            start_time10 = datetime.now()
            print(f"\n[{start_time10}] What was the longest accident (in hours) recorded in Las Vegas in the Spring (March, April, and May)? Display the data per year.\n")
            location_name = 'Las Vegas'
            spring_months = [3, 4, 5]
            df_q10 = q10_answer(df_cleaned, location_name, spring_months)
            print(f"The longest accident (in hours) recorded in Las Vegas in the Spring: \n{df_q10}\n")
            print(Color.RESET)
            query_time = (datetime.now() - start_time1).total_seconds()
            total_time += query_time/60

    elif choice == '4':
        if df_cleaned is None or len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print(Color.GREEN)
            print("Search Accidents: ")
            print("********************************************")
            start_time = datetime.now()
            res, df_filter_again = search_accidents_by_location(df_cleaned)
            print(f"\nThere were {res} accidents.\n")
            search_time = (datetime.now() - start_time).total_seconds()
            total_time += search_time/60
            print(f"Time to perform search is: {search_time}\n")
            print(Color.RESET)

    elif choice == '5':
        if df_cleaned is None or len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print(Color.GREEN)
            print("Search Accidents: ")
            print("********************************************")
            start_time = datetime.now()
            res = search_accidents_by_date(df_cleaned)
            print(f"\nThere were {res} accidents.\n")
            search_time = (datetime.now() - start_time).total_seconds()
            total_time += search_time/60
            print(f"Time to perform search is: {search_time}\n")
            print(Color.RESET)

    elif choice == '6':
        if df_cleaned is None or len(df_cleaned) == 0:
            print ("\nPlease process data firstly!\n")
        else:
            print(Color.GREEN)
            print("Search Accidents: ")
            print("********************************************")
            start_time = datetime.now()
            res = search_accidents_by_conditions(df_cleaned)
            print(f"\nThere were {res} accidents.\n")
            search_time = (datetime.now() - start_time).total_seconds()
            total_time += search_time/60
            print(f"Time to perform search is: {search_time}\n")
            print(Color.RESET)

    elif choice == '7':
        print(Color.GREEN)
        print(f" Total Running Time (In Minutes): {total_time}")
        print(Color.RESET)
        quit_program()

    else:
        print("Invalid choice. Please enter a number from 1 to 7.\n")

