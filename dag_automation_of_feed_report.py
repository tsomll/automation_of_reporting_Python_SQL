from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse

# подключение к бд
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20250220'
}

def metrics_report(chat=None):
    chat_id=chat if chat else '818972037' 
    my_token = '8152376546:AAHJDyX118SFFsp0VQqZqNLJP5HmHsnksrc' # токен бота
    bot = telegram.Bot(token=my_token) # получаем доступ
    #запрос в бд для вывода метрик
    q = """
    SELECT toDate(time) as day,
        count(DISTINCT user_id) as dau,
        sum(action = 'view') as views,
        sum(action = 'like') as likes,
        likes/views as ctr
    FROM {db}.feed_actions 
    WHERE toDate(time) BETWEEN today()-7 and  today()-1
    GROUP BY toDate(time)
    """

    df = pandahouse.read_clickhouse(q, connection=connection)

    #построение графика с ключевыми метриками
    df_scaled = df.copy()

    #нормализация ключевых метрик
    for col in ['dau', 'views', 'likes', 'ctr']:
        df_scaled[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_scaled, x='day', y='dau', label='DAU')
    sns.lineplot(data=df_scaled, x='day', y='views', label='Views')
    sns.lineplot(data=df_scaled, x='day', y='likes', label='Likes')
    sns.lineplot(data=df_scaled, x='day', y='ctr', label='CTR')

    plt.title('Нормализированные ключевые метрики за предыдущие 7 дней')
    plt.xlabel("Дата")
    plt.ylabel("Метрики")  
    plt.legend()
    plot_object = io.BytesIO()
    plt.savefig(plot_object,format='png')

    #отправка сообщения с графиком
    plot_object.seek(0)
    plot_object.name = 'metrics_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    plot_object.close() 

    # текст с информацией о значениях ключевых метрик за предыдущий день
    msg = (f"*Отчет за {df['day'][6].strftime('%Y-%m-%d')}*\n"
           f"*DAU:* {df['dau'][6]}\n"
           f"*Просмотры:* {df['views'][6]}\n"
           f"*Лайки:* {df['likes'][6]}\n"
           f"*CTR:* {df['ctr'][6]:.2%}")  

    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="Markdown")
    

# параметры dag
default_args = {
    'owner': 'adel.valishina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 29),
}

# интервал запуска
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_of_feed():
    
    @task()
    def make_metrics_report():
        metrics_report()
    make_metrics_report()
    
dag_report_of_feed = dag_report_of_feed()
