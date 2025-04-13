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

def generate_action_metrics():
    q_dau_yesterday = """
    SELECT 
        DATE(time) AS activity_date, 
        COUNT(DISTINCT user_id) AS dau,
        sum(action = 'view') as views,
        sum(action = 'like') as likes,
        likes/views as ctr

    FROM simulator_20250220.feed_actions
    WHERE toDate(time) = today() - 1
    GROUP BY DATE(time)
    ORDER BY activity_date DESC;
    """

    df_dau_yesterday = pandahouse.read_clickhouse(q_dau_yesterday, connection=connection)

    q_dau_week_ago = """
    SELECT 
        DATE(time) AS activity_date, 
        COUNT(DISTINCT user_id) AS dau,
        sum(action = 'view') as views,
        sum(action = 'like') as likes,
        likes/views as ctr
    FROM simulator_20250220.feed_actions
    WHERE toDate(time) = today() - 8
    GROUP BY DATE(time)
    ORDER BY activity_date DESC;
    """

    df_dau_week_ago = pandahouse.read_clickhouse(q_dau_week_ago, connection=connection)

    # метрики за вчерашний день к аналогичному дню неделю назад
    dau_growth = (df_dau_yesterday['dau'][0] - df_dau_week_ago['dau'][0]) / df_dau_week_ago['dau'][0] * 100

    views_growth = (df_dau_yesterday['views'][0] - df_dau_week_ago['views'][0]) / df_dau_week_ago['views'][0] * 100
    
    likes_growth = (df_dau_yesterday['likes'][0] - df_dau_week_ago['likes'][0]) / df_dau_week_ago['likes'][0] * 100

    ctr_growth = (df_dau_yesterday['ctr'][0] - df_dau_week_ago['ctr'][0]) / df_dau_week_ago['ctr'][0] * 100

    msg_action = (
        f"*Статистика ленты новостей:\n*"
       f"*DAU:* {df_dau_yesterday['dau'][0]} (прирост: {dau_growth:.2f} %)\n"
       f"*Просмотры:* {df_dau_yesterday['views'][0]} (прирост: {views_growth:.2f} %)\n"
       f"*Лайки:* {df_dau_yesterday['likes'][0]} (прирост: {likes_growth:.2f} %)\n"
       f"*CTR:* {df_dau_yesterday['ctr'][0]:.2f} % (прирост: {ctr_growth:.2f} %)") 
    return msg_action

def generate_message_metrics():
    q_message_dau_yesterday = """
    SELECT
        DATE(time) AS activity_date, 
        COUNT(DISTINCT user_id) AS dau,
        COUNT(*) as count_messages,
        COUNT(*) / COUNT(DISTINCT user_id) AS avg_messages_per_user
    FROM simulator_20250220.message_actions
    WHERE toDate(time) = today() - 1
    GROUP BY DATE(time)
    ORDER BY activity_date DESC;
    """


    df_message_dau_yesterday = pandahouse.read_clickhouse(q_message_dau_yesterday, connection=connection)

    q_message_dau_week_ago = """
    SELECT
        DATE(time) AS activity_date, 
        COUNT(DISTINCT user_id) AS dau,
        COUNT(*) as count_messages,
        COUNT(*) / COUNT(DISTINCT user_id) AS avg_messages_per_user
    FROM simulator_20250220.message_actions
    WHERE toDate(time) = today() - 8
    GROUP BY DATE(time)
    ORDER BY activity_date DESC;
    """

    # процент пользователей, отправивших хотя бы одно сообщение
    q_dau_action = """
        SELECT 
            COUNT(DISTINCT user_id) AS dau_total
        FROM (
            SELECT DISTINCT user_id FROM simulator_20250220.feed_actions WHERE toDate(time) = today() - 1
            UNION ALL
            SELECT DISTINCT user_id FROM simulator_20250220.message_actions WHERE toDate(time) = today() - 1
        ) AS unique_users;
        """
    df_dau_total = pandahouse.read_clickhouse(q_dau_action, connection=connection)

    percent_users_messaging = (df_message_dau_yesterday['dau'][0] / df_dau_total['dau_total'][0]) * 100

    df_message_dau_week_ago = pandahouse.read_clickhouse(q_message_dau_week_ago, connection=connection)

    dau_growth = (df_message_dau_yesterday['dau'][0] - df_message_dau_week_ago['dau'][0]) / df_message_dau_week_ago['dau'][0] * 100

    count_messages_growth = (df_message_dau_yesterday['count_messages'][0] - df_message_dau_week_ago['count_messages'][0]) / df_message_dau_week_ago['count_messages'][0] * 100

    avg_message_growth = (df_message_dau_yesterday['avg_messages_per_user'][0] - df_message_dau_week_ago['avg_messages_per_user'][0]) / df_message_dau_week_ago['avg_messages_per_user'][0] * 100

    msg_action = (
        f"*Статистика мессенджера:\n*"
       f"*DAU:* {df_message_dau_yesterday['dau'][0]} (прирост: {dau_growth:.2f} %)\n"
       f"*Количество отправленных сообщений:* {df_message_dau_yesterday['count_messages'][0]} (прирост: {count_messages_growth:.2f} %)\n"
       f"*Среднее количество отправленных сообщений на человека:* {df_message_dau_yesterday['avg_messages_per_user'][0]:.2f} (прирост: {avg_message_growth:.2f} %)\n"
       f"*Процент пользователей, отправивших хотя бы одно сообщение:* {percent_users_messaging:.2f} %\n")
    return msg_action

def generate_retenshion():
    q_gone="""SELECT toStartOfDay(toDateTime(this_week)) AS __timestamp,
           status AS status,
           AVG(num_users) AS "AVG(num_users)"
    FROM
      (SELECT this_week,
              previous_week, -uniq(user_id) as num_users,
                              status
       FROM
         (SELECT user_id,
                 groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                 addWeeks(arrayJoin(weeks_visited),+1) as this_week,
                 if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
                 addWeeks(this_week, -1) as previous_week
          FROM simulator_20250220.feed_actions
          group by user_id)
       where status='gone'
       GROUP BY this_week,
                previous_week,
                status
       HAVING this_week != addWeeks(toMonday(today()),+1)
       union all SELECT this_week,
                        previous_week,
                        toInt64(uniq(user_id)) as num_users,
                        status
       FROM
         (SELECT user_id,
                 groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                 arrayJoin(weeks_visited) as this_week,
                 if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
                 addWeeks(this_week, -1) as previous_week
          FROM simulator_20250220.feed_actions
          group by user_id)
       GROUP BY this_week,
                previous_week,
                status) AS virtual_table
    GROUP BY status,
             toStartOfDay(toDateTime(this_week))
    ORDER BY "AVG(num_users)" DESC"""
    df_gone_reten = pandahouse.read_clickhouse(q_gone, connection=connection)

    df_gone_reten["__timestamp"] = pd.to_datetime(df_gone_reten["__timestamp"])

    this_week_start = pd.to_datetime("today") - pd.Timedelta(days=pd.to_datetime("today").weekday())
    this_week_start = this_week_start.normalize()  # Приводим к началу дня

    df_last_week = df_gone_reten[df_gone_reten["__timestamp"] == this_week_start]

    new_users = df_last_week[df_last_week["status"] == "new"]["AVG(num_users)"].sum()
    gone_users = df_last_week[df_last_week["status"] == "gone"]["AVG(num_users)"].sum()
    retained_users = df_last_week[df_last_week["status"] == "retained"]["AVG(num_users)"].sum()
    
    msg_reten = (
        f"*Статистика по всему приложению: \n*"
       f"*{new_users}* новых пользователей\n"
       f"*{retained_users}* старых пользователей\n"
       f"*{gone_users * (-1)}* ушедших пользователей\n")
    return msg_reten

def generate_graph_retenshion():
    q_retenshion = """SELECT toStartOfDay(toDateTime(this_week)) AS __timestamp,
                       status AS status,
                       AVG(num_users) AS "AVG(num_users)"
                FROM
                  (SELECT this_week,
                          previous_week, -uniq(user_id) as num_users,
                          status
                   FROM
                     (SELECT user_id,
                             groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                             addWeeks(arrayJoin(weeks_visited),+1) as this_week,
                             if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
                             addWeeks(this_week, -1) as previous_week
                      FROM simulator_20250220.feed_actions
                      group by user_id)
                   where status='gone'
                   GROUP BY this_week,
                            previous_week,
                            status
                   HAVING this_week != addWeeks(toMonday(today()),+1)
                   union all SELECT this_week,
                                previous_week,
                                toInt64(uniq(user_id)) as num_users,
                                status
                   FROM
                     (SELECT user_id,
                             groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                             arrayJoin(weeks_visited) as this_week,
                             if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
                             addWeeks(this_week, -1) as previous_week
                      FROM simulator_20250220.feed_actions
                      group by user_id)
                   GROUP BY this_week,
                            previous_week,
                            status) AS virtual_table
                GROUP BY status,
                         toStartOfDay(toDateTime(this_week))
                ORDER BY "AVG(num_users)" DESC"""

    df_reten = pandahouse.read_clickhouse(q_retenshion, connection=connection)

    df_reten['__timestamp'] = pd.to_datetime(df_reten['__timestamp'])
    df_reten.set_index('__timestamp', inplace=True)

    df_pivot = df_reten.pivot_table(index=df_reten.index, columns='status', values='AVG(num_users)', aggfunc='mean')

    df_pivot.plot(kind='bar', stacked=True, figsize=(12, 6), color=['salmon', 'lightblue', 'lightgreen'])

    plt.title('Аудитория по неделям')
    plt.xlabel('Дата')
    plt.ylabel('Количество пользователей')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return plt

def generate_report(chat=None):
    chat_id=chat if chat else '818972037' 
    my_token = '8152376546:AAHJDyX118SFFsp0VQqZqNLJP5HmHsnksrc' # токен бота
    bot = telegram.Bot(token=my_token) 

    msg_action = generate_action_metrics()
    bot.sendMessage(chat_id=chat_id, text=msg_action, parse_mode="Markdown")

    msg_message = generate_message_metrics()
    bot.sendMessage(chat_id=chat_id, text=msg_message, parse_mode="Markdown")

    msg_reten = generate_retenshion()
    bot.sendMessage(chat_id=chat_id, text=msg_reten, parse_mode="Markdown")

    plt=generate_graph_retenshion()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)

    plot_object.seek(0)
    plot_object.name = 'metrics_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
# параметры dag
default_args = {
    'owner': 'adel.valishina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 30),
}

# интервал запуска
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_application_report():
    
    @task()
    def make_report():
        generate_report()
    make_report()
    
dag_application_report = dag_application_report()
