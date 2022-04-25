
#import libraries
import requests
import pandas as pd
import time
import json
from datetime import datetime
import psycopg2
import config



#Generated API KEY in Config File and credientials for the DB 

city_ids = ['4180439','4671654','4930956','5913490','5920288','4887398','4684888','5419384','4990729','5946768','5957776','6324729','4699066','5983720','5506956','5368361','4164138','5263045','5037649','6075357','6077243','4644585','4335045','5128638','6087892','4544349','4167147','6094817','4560349''5308655','5206379','5746545','6119109','5391959','5809844','6324733','4407066','6167865','6173331','6174041','4140963','6180550','6183235','6185377']

conn = psycopg2.connect(dbname= config.DB_NAME, user= config.DB_USER, password= config.DB_PASS , host= config.DB_HOST, port = config.DB_PORT)

cursor = conn.cursor()
print("connection opened")


#created the pandas dataframe with the required columns

def create_df():
    
    df = pd.DataFrame(columns = ['DateTime',
    'Longitude',
    'Latitude',
    'Weather_Condition',
    'Weather_Condition_Description',
    'Temperature',
    'Minimum_Tempature',
    'Maximum_Tempature',
    'Humidity',
    'Windspeed',
    'City',
    'Country',])

    return df


#Created a function to obtain the current time into a specific format 
def date_and_time():
    now = datetime.now() # current date and time
    date_time = now.strftime("%m/%d/%Y %H:%M:%S")
    print("date and time:",date_time)
    
    return date_time



def insert_values_df(df, date_time):
    for city in city_ids:

        currentweatherurl = "https://api.openweathermap.org/data/2.5/weather?id="+city+"&appid="+config.API_KEY+"&units=metric"

        currentweather = requests.get(currentweatherurl).json()

        current_location_long = currentweather['coord']['lon']
        current_location_lat = currentweather['coord']['lat']
        current_weather_cond = currentweather['weather'][0]['main']
        current_weather_cond_desc = currentweather['weather'][0]['description']
        current_temp = currentweather['main']['temp']
        current_temp_min = currentweather['main']['temp_min']
        current_temp_max = currentweather['main']['temp_max']
        current_humid = currentweather['main']['humidity']
        current_windspeed = currentweather['wind']['speed']
        current_city = currentweather['name']
        current_country = currentweather['sys']['country']

        df = df.append({
    'DateTime': date_time,
    'Longitude': current_location_long,  
    'Latitude': current_location_lat,
    'Weather_Condition': current_weather_cond,
    'Weather_Condition_Description': current_weather_cond_desc, 
    'Temperature': current_temp, 
    'Minimum_Tempature': current_temp_min,
    'Maximum_Tempature': current_temp_max,
    'Humidity': current_humid,
    'Windspeed': current_windspeed,
    'City': current_city , 
    'Country': current_country} , ignore_index = True)
        
        
    
    return df


def append_df(cursor,df):
    
    tmp_df = pd.DataFrame(columns = ['DateTime',
    'Longitude',
    'Latitude',
    'Weather_Condition',
    'Weather_Condition_Description',
    'Temperature',
    'Minimum_Tempature',
    'Maximum_Tempature',
    'Humidity',
    'Windspeed',
    'City',
    'Country',])
    
    for i, row in df.iterrows():
        tmp_df = tmp_df.append(row)
        
    return tmp_df

cursor.execute('drop table if exists weather;')
createtable = 'Create table If Not Exists weather2 (DateTime timestamp,Longitude int,Latitude int, Weather_Condition varchar(200), Weather_Condition_Description varchar(200), Temperature float, Minimum_Tempature float, Maximum_Tempature float, Humidity int,Windspeed float,City varchar(200),Country varchar(200));'

cursor.execute(createtable)



def insert_into_table(cursor, DateTime,Longitude,Latitude,Weather_Condition,Weather_Condition_Description,Temperature,Minimum_Tempature,Maximum_Tempature,Humidity,Windspeed,City,Country):
    insert_script = 'insert into weather2 (DateTime,Longitude,Latitude,Weather_Condition,Weather_Condition_Description,Temperature,Minimum_Tempature,Maximum_Tempature,Humidity,Windspeed,City,Country) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    values_to_insert = (DateTime,
    Longitude,
    Latitude,
    Weather_Condition,
    Weather_Condition_Description,
    Temperature,
    Minimum_Tempature,
    Maximum_Tempature,
    Humidity,
    Windspeed,
    City,
    Country)
    
    cursor.execute(insert_script, values_to_insert)


main_df = create_df()
dates_times = date_and_time()

main_df = insert_values_df(main_df, dates_times)

for i, row in main_df.iterrows():
    insert_into_table(cursor, row['DateTime'], row['Longitude'], row['Latitude'],row['Weather_Condition'],row['Weather_Condition_Description'],row['Temperature'],row['Minimum_Tempature'],row['Maximum_Tempature'],row['Humidity'],row['Windspeed'],row['City'],row['Country'])

conn.commit()
conn.close()

