import streamlit as st
import pandas as pd
from preprocessing import preprocess_data, async_get_weather_data, create_season_profile, get_season, async_analyze_anomaly
import asyncio
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import time

def main():
    uploaded_file = st.file_uploader("Выберите файл", type=["csv", "xlsx"])

    if uploaded_file is not None:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)

        st.write("Загруженный файл:")
        st.write(df)
        df = preprocess_data(df)
        
        cities = [''] + df['city'].unique().tolist()
        city_selected = st.selectbox("Выберите город", cities)
        current_temp = None
        with st.form(key='input_form'):
            API_key = st.text_input("Введите API ключ")
            submit_button = st.form_submit_button(label='Получить текущую температуру и проверить на аномалию')
            if submit_button:
                status, current_temp = asyncio.run(async_get_weather_data(city_selected, API_key))
                if status != 200: 
                    st.write(current_temp)
                st.write(df[df['city']==city_selected].describe())

        if city_selected:
            df_city = df[df['city'] == city_selected]
            df_city['timestamp'] = pd.to_datetime(df_city['timestamp'])
            # Строим график изменения температуры в течение времени
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df_city['timestamp'], y=df_city['temperature'], mode='lines', name='Temperature'))
            df_city_anomalies = df_city[df_city['is_anomaly']]
            fig.add_trace(go.Scatter(x=df_city_anomalies['timestamp'], y=df_city_anomalies['temperature'], mode='markers', name='Anomalies', marker=dict(color='red')))
            fig.update_layout(title=f'Изменение температуры в {city_selected} в течение времени', xaxis_title='Время', yaxis_title='Температура')
            fig.update_xaxes(rangeslider_visible=True)
            st.plotly_chart(fig)
            
            #график сезонных профилей
            fig = go.Figure()
            profiles_data = create_season_profile(city_selected,df)
            seasonal_stats = profiles_data['seasonal_stats']
            #print(seasonal_stats)
            seasons = seasonal_stats['season']
            
            #средняя температура за сезон
            fig.add_trace(go.Scatter(
                x=seasons,
                y=seasonal_stats['mean_temp'],
                mode='markers',
                name='Средняя температура за сезон',
                line=dict(color='blue'),
            ))
            #среднее и разброс
            for i in range(len(seasonal_stats)):
                fig.add_trace(go.Scatter(
                    x=[seasons[i], seasons[i]],
                    y=[seasonal_stats.iloc[i]['mean_temp'] - seasonal_stats.iloc[i]['std_temp'], 
                    seasonal_stats.iloc[i]['mean_temp'] + seasonal_stats.iloc[i]['std_temp']],
                    mode='markers',
                    name=f"Разброс стандартного отклонения ({seasonal_stats['season'][i]})",
                    line=dict(color='gray', width=2),
                ))
            #минимальная температура за сезон
            fig.add_trace(go.Scatter(
                x=seasons,
                y=seasonal_stats['min_temp'],
                mode='markers',
                name='Минимальная температура за сезон',
                line=dict(color='red'),
            ))
            #максимальная температура за сезон
            fig.add_trace(go.Scatter(
                x=seasons,
                y=seasonal_stats['max_temp'],
                mode='markers',
                name='Максимальная температура за сезон',
                line=dict(color='red'),
            ))
            
            fig.update_layout(
                title="Средняя температура по сезонам со средним и стандартным отклонением  в городе " + city_selected,
                xaxis_title="Сезон",
                yaxis_title="Температура (°C)",
                template="plotly_white"
            )
            if current_temp and API_key and city_selected:
                current_month = time.localtime().tm_mon # текущий месяц
                season = asyncio.run(get_season(current_month)) # текущий сезон
                fig.add_trace(go.Scatter(
                    x=[season],
                    y=[current_temp],
                    mode='markers',
                    name='Текущая температура',
                    line=dict(color='green'),
                ))
                res_anomaly = asyncio.run(async_analyze_anomaly(city_selected, API_key, df))
                st.write(f'Текущая температура в городе {city_selected}: {current_temp} градусов по Цельсию. Аномалия: {res_anomaly}')
            
            st.plotly_chart(fig)
            
            
    else:
        st.write("Пожалуйста, загрузите файл.")
        
if __name__ == "__main__":
    main()