import pandas as pd
import numpy as np
import aiohttp 
import time

def calculate_season_mean_sdt_analyze_anomalies(df):
    df['timestamp'] = pd.to_datetime(df['timestamp']) #переводим в формат даты для того, чтобы сформировать столбец сезонов
    df['season'] = df['timestamp'].dt.month.apply(lambda x: 'winter' if x in [12, 1, 2] else 'spring' if x in [3, 4, 5] else 'summer' if x in [6, 7, 8] else 'autumn')
    #среднее для профиля сезона
    seasonal_stats = df.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).rename(columns={'mean': 'season_mean', 'std': 'season_std'}).reset_index() 
    #среднее для аномалий
    df['rolling_mean'] = df.groupby(['city'])['temperature'].rolling(window=30).mean().reset_index(level=0, drop=True)
    df['rolling_std'] = df.groupby(['city'])['temperature'].rolling(window=30).std().reset_index(level=0, drop=True)
    # Определение аномалий
    df['is_anomaly'] = (df['temperature'] < df['rolling_mean'] - 2 * df['rolling_std']) | (df['temperature'] > df['rolling_mean'] + 2 * df['rolling_std'])
    df = df.merge(seasonal_stats, on=['city', 'season'], how='left')
    return df

def preprocess_data(df):
    #df = pd.read_csv(df)
    df = calculate_season_mean_sdt_analyze_anomalies(df)
    return df


async def async_get_weather_data(city, API_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_key}&units=metric"
    try: 
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                # if response.status != 200:  # Другие ошибки сервера
                #     return response
                data = await response.json()
                if data.get('cod') != 200:
                    return response.status, data
                temp = data.get('main').get('temp')                
                return response.status, temp
    except Exception as e: 
        raise RuntimeError(f"Ошибка при получении данных о погоде: {e}")
        
async def get_season(month):
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'autumn'

async def async_analyze_anomaly(city, API_key, df):
   status, temp = await async_get_weather_data(city, API_key)
   print(f'{city} temp is {temp}')
   current_month = time.localtime().tm_mon # текущий месяц
   season = await get_season(current_month) # текущий сезон
   print(season)
   city_data = df[(df['city'] == city) & (df['season'] == season)]
   mean_temp_by_city = city_data['season_mean'].values[0]
   print(f'{city} mean temp is {mean_temp_by_city}')
   season_std_by_city = city_data['season_std'].values[0]
   print(f'{city} std temp is {season_std_by_city}')
   anomaly_flag = False
   if (temp > (mean_temp_by_city + 2 * season_std_by_city)) or (temp < (mean_temp_by_city - 2 * season_std_by_city)):
      anomaly_flag = True
      print(anomaly_flag)
      return anomaly_flag
   else: 
      print(anomaly_flag)
      return anomaly_flag
  
from sklearn.linear_model import LinearRegression
  
def create_season_profile(city,df):
    df_city = df[df['city']==city]
    seasonal_stats = df_city.groupby('season')['temperature'].agg(
        min_temp='min',
        max_temp='max',
        mean_temp='mean',
        std_temp='std'
    ).reset_index()
    print(seasonal_stats)
    whole_time_mean = df_city['temperature'].mean()
    whole_time_min = df_city['temperature'].min()
    whole_time_max = df_city['temperature'].max()
    
    df_city['timestamp_num'] = df_city['timestamp'].map(pd.Timestamp.toordinal)  # Преобразуем дату в числовой формат
    model = LinearRegression()
    model.fit(df_city[['timestamp_num']], df_city['temperature'])
    trend_slope = model.coef_[0]  # Уклон тренда (положительный или отрицательный)
    
    return {
        'city': city,
        'seasonal_stats': seasonal_stats,
        'whole_time_mean': whole_time_mean,
        'whole_time_min': whole_time_min,
        'whole_time_max': whole_time_max,
        'trend_slope': trend_slope
    }