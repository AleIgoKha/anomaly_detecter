import pandahouse as ph
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram as tg

from datetime import date, datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'a.harchenko-16',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 6),
}

schedule_interval = '1-59/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def anomaly_reporter():
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def anomaly_detecter(connection):
        query_dau_feed = """
        WITH
        -- calculating active users every 15 minutes
        date_time_users AS 
            (SELECT toDate(time) AS date,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    uniqExact(user_id) AS users
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) < today()
            GROUP BY toDate(time), toStartOfInterval(time, toIntervalMinute(15))),

        -- calculating average users number throughout a day by 15 minutes interval for every day
        date_time_average_users AS 
            (SELECT date,
                    time_fifteen AS time,
                    users,
                    AVG(users) OVER (PARTITION BY date) AS avg_users
            FROM date_time_users),

        -- calculating the relative deviation of each user number
        -- from the average value related to its day
        relative_deviation_table AS 
            (SELECT date,
                    time,
                    users / avg_users AS relative_deviation
            FROM date_time_average_users),

        -- calculating the confidence interval of the relative
        -- deviations for each 15-minute interval
        conf_int_table AS 
            (SELECT time,
                    AVG(relative_deviation) - 3 * stddevSamp(relative_deviation) AS lower_bound,
                    AVG(relative_deviation) + 3 * stddevSamp(relative_deviation) AS upper_bound,
                    AVG(relative_deviation) AS avg_relative_deviation
            FROM relative_deviation_table
            GROUP BY time),

        -- calculating the weights for every average users number 
        -- throughout a day by 15 minutes interval for every day
        date_weights_table AS 
            (SELECT date,
                    toDayOfWeek(date) AS weekday,
                    avg_users,
                    ROW_NUMBER() OVER (PARTITION BY toDayOfWeek(date) ORDER BY date) AS date_weight
            FROM date_time_average_users
            GROUP BY date,
                    toDayOfWeek(date) AS weekday,
                    avg_users),

        -- calculating the weighted average of every average users number
        -- throughout a day by 15 minutes interval for every day for every weekday
        weighted_avg_calculation AS 
            (SELECT weekday,
                    MAX(weighted_avgs) / MAX(weights_sum) AS weighted_avg
            FROM
                (SELECT weekday,
                        SUM(avg_users * date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weighted_avgs,
                        SUM(date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weights_sum
                FROM date_weights_table)
            GROUP BY weekday),

        -- calculating number of active users for the last 15 minutes interval
        users_now AS
            (SELECT toDate(time) AS date,
                    toDayOfWeek(time) AS weekday,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    uniqExact(user_id) AS users
            FROM simulator_20250120.feed_actions
            WHERE toStartOfInterval(time, toIntervalMinute(15)) = toStartOfInterval(now() - toIntervalMinute(15), toIntervalMinute(15))
            GROUP BY toDate(time), toDayOfWeek(time), toStartOfInterval(time, toIntervalMinute(15)))

        SELECT 'Number of Active Feed Users' AS metric_name,
                time,
                users / weighted_avg AS relative_deviation,
                lower_bound,
                upper_bound,
                avg_relative_deviation,
                (ROUND(avg_relative_deviation * weighted_avg)::int)::String AS avg_expected_value,
                users::String AS metric_value,
                ROUND((users * 100) / (avg_relative_deviation * weighted_avg) - 100, 2)::String AS change
        FROM users_now JOIN weighted_avg_calculation USING(weekday)
        JOIN conf_int_table ON conf_int_table.time = users_now.time_fifteen
        WHERE relative_deviation NOT BETWEEN lower_bound AND upper_bound
        """
    
        query_dau_messenger = """
        WITH
        -- calculating active users every 15 minutes
        date_time_users AS 
            (SELECT toDate(time) AS date,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    uniqExact(user_id) AS users
            FROM simulator_20250120.message_actions
            WHERE toDate(time) < today()
            GROUP BY toDate(time), toStartOfInterval(time, toIntervalMinute(15))),

        -- calculating average users number throughout a day by 15 minutes interval for every day
        date_time_average_users AS 
            (SELECT date,
                    time_fifteen AS time,
                    users,
                    AVG(users) OVER (PARTITION BY date) AS avg_users
            FROM date_time_users),

        -- calculating the relative deviation of each user number
        -- from the average value related to its day
        relative_deviation_table AS 
            (SELECT date,
                    time,
                    users / avg_users AS relative_deviation
            FROM date_time_average_users),

        -- calculating the confidence interval of the relative
        -- deviations for each 15-minute interval
        conf_int_table AS 
            (SELECT time,
                    AVG(relative_deviation) - 3 * stddevSamp(relative_deviation) AS lower_bound,
                    AVG(relative_deviation) + 3 * stddevSamp(relative_deviation) AS upper_bound,
                    AVG(relative_deviation) AS avg_relative_deviation,
                    median(relative_deviation) AS median_relative_deviation
            FROM relative_deviation_table
            GROUP BY time),

        -- calculating the weights for every average users number 
        -- throughout a day by 15 minutes interval for every day
        date_weights_table AS 
            (SELECT date,
                    toDayOfWeek(date) AS weekday,
                    avg_users,
                    ROW_NUMBER() OVER (PARTITION BY toDayOfWeek(date) ORDER BY date) AS date_weight
            FROM date_time_average_users
            GROUP BY date,
                    toDayOfWeek(date) AS weekday,
                    avg_users),

        -- calculating the weighted average of every average users number
        -- throughout a day by 15 minutes interval for every day for every weekday
        weighted_avg_calculation AS 
            (SELECT weekday,
                    MAX(weighted_avgs) / MAX(weights_sum) AS weighted_avg
            FROM
                (SELECT weekday,
                        SUM(avg_users * date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weighted_avgs,
                        SUM(date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weights_sum
                FROM date_weights_table)
            GROUP BY weekday),

        -- calculating number of active users for the last 15 minutes interval
        users_now AS
            (SELECT toDate(time) AS date,
                    toDayOfWeek(time) AS weekday,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    uniqExact(user_id) AS users
            FROM simulator_20250120.message_actions
            WHERE toStartOfInterval(time, toIntervalMinute(15)) = toStartOfInterval(now() - toIntervalMinute(15), toIntervalMinute(15))
            GROUP BY toDate(time), toDayOfWeek(time), toStartOfInterval(time, toIntervalMinute(15)))

        SELECT 'Number of Active Messenger Users' AS metric_name,
                time,
                users / weighted_avg AS relative_deviation,
                lower_bound,
                upper_bound,
                avg_relative_deviation,
                (ROUND(avg_relative_deviation * weighted_avg)::int)::String AS avg_expected_value,
                users::String AS metric_value,
                ROUND((users * 100) / (avg_relative_deviation * weighted_avg) - 100, 2) AS change
        FROM users_now JOIN weighted_avg_calculation USING(weekday)
        JOIN conf_int_table ON conf_int_table.time = users_now.time_fifteen
        WHERE relative_deviation NOT BETWEEN lower_bound AND upper_bound
        """

        query_views = """
        WITH
        -- calculating views number every 15 minutes
        date_time_views AS 
            (SELECT toDate(time) AS date,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    SUM(action = 'view') AS views
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) < today()
            GROUP BY toDate(time), toStartOfInterval(time, toIntervalMinute(15))),

        -- calculating average views number throughout a day by 15 minutes interval for every day
        date_time_average_views AS 
            (SELECT date,
                    time_fifteen AS time,
                    views,
                    AVG(views) OVER (PARTITION BY date) AS avg_views
            FROM date_time_views),

        -- calculating the relative deviation of each views number
        -- from the average value related to its day
        relative_deviation_table AS 
            (SELECT date,
                    time,
                    views / avg_views AS relative_deviation
            FROM date_time_average_views),

        -- calculating the confidence interval of the relative
        -- deviations for each 15-minute interval
        conf_int_table AS 
            (SELECT time,
                    AVG(relative_deviation) - 3 * stddevSamp(relative_deviation) AS lower_bound,
                    AVG(relative_deviation) + 3 * stddevSamp(relative_deviation) AS upper_bound,
                    AVG(relative_deviation) AS avg_relative_deviation,
                    median(relative_deviation) AS median_relative_deviation
            FROM relative_deviation_table
            GROUP BY time),

        -- calculating the weights for every average views number 
        -- throughout a day by 15 minutes interval for every day
        date_weights_table AS 
            (SELECT date,
                    toDayOfWeek(date) AS weekday,
                    avg_views,
                    ROW_NUMBER() OVER (PARTITION BY toDayOfWeek(date) ORDER BY date) AS date_weight
            FROM date_time_average_views
            GROUP BY date,
                    toDayOfWeek(date) AS weekday,
                    avg_views),

        -- calculating the weighted average of every average views number
        -- throughout a day by 15 minutes interval for every day for every weekday
        weighted_avg_calculation AS 
            (SELECT weekday,
                    MAX(weighted_avgs) / MAX(weights_sum) AS weighted_avg
            FROM
                (SELECT weekday,
                        SUM(avg_views * date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weighted_avgs,
                        SUM(date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weights_sum
                FROM date_weights_table)
            GROUP BY weekday),

        -- calculating views number for the last 15 minutes interval
        views_now AS
            (SELECT toDate(time) AS date,
                    toDayOfWeek(time) AS weekday,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    SUM(action = 'view') AS views
            FROM simulator_20250120.feed_actions
            WHERE toStartOfInterval(time, toIntervalMinute(15)) = toStartOfInterval(now() - toIntervalMinute(15), toIntervalMinute(15))
            GROUP BY toDate(time), toDayOfWeek(time), toStartOfInterval(time, toIntervalMinute(15)))

        SELECT 'Number of User Views' AS metric_name,
                time,
                views / weighted_avg AS relative_deviation,
                lower_bound,
                upper_bound,
                avg_relative_deviation,
                (ROUND(avg_relative_deviation * weighted_avg)::int)::String AS avg_expected_value,
                views::String AS metric_value,
                ROUND((views * 100) / (avg_relative_deviation * weighted_avg) - 100, 2)::String AS change
        FROM views_now JOIN weighted_avg_calculation USING(weekday)
        JOIN conf_int_table ON conf_int_table.time = views_now.time_fifteen
        WHERE relative_deviation NOT BETWEEN lower_bound AND upper_bound
        """
        
        query_likes = """
        WITH
        -- calculating likes number every 15 minutes
        date_time_likes AS 
            (SELECT toDate(time) AS date,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    SUM(action = 'like') AS likes
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) < today()
            GROUP BY toDate(time), toStartOfInterval(time, toIntervalMinute(15))),

        -- calculating average likes number throughout a day by 15 minutes interval for every day
        date_time_average_likes AS 
            (SELECT date,
                    time_fifteen AS time,
                    likes,
                    AVG(likes) OVER (PARTITION BY date) AS avg_likes
            FROM date_time_likes),

        -- calculating the relative deviation of each likes number
        -- from the average value related to its day
        relative_deviation_table AS 
            (SELECT date,
                    time,
                    likes / avg_likes AS relative_deviation
            FROM date_time_average_likes),

        -- calculating the confidence interval of the relative
        -- deviations for each 15-minute interval
        conf_int_table AS 
            (SELECT time,
                    AVG(relative_deviation) - 3 * stddevSamp(relative_deviation) AS lower_bound,
                    AVG(relative_deviation) + 3 * stddevSamp(relative_deviation) AS upper_bound,
                    AVG(relative_deviation) AS avg_relative_deviation,
                    median(relative_deviation) AS median_relative_deviation
            FROM relative_deviation_table
            GROUP BY time
            ORDER BY time),

        -- calculating the weights for every average likes number 
        -- throughout a day by 15 minutes interval for every day
        date_weights_table AS 
            (SELECT date,
                    toDayOfWeek(date) AS weekday,
                    avg_likes,
                    ROW_NUMBER() OVER (PARTITION BY toDayOfWeek(date) ORDER BY date) AS date_weight
            FROM date_time_average_likes
            GROUP BY date,
                    toDayOfWeek(date) AS weekday,
                    avg_likes),

        -- calculating the weighted average of every average likes number
        -- throughout a day by 15 minutes interval for every day for every weekday
        weighted_avg_calculation AS 
            (SELECT weekday,
                    MAX(weighted_avgs) / MAX(weights_sum) AS weighted_avg
            FROM
                (SELECT weekday,
                        SUM(avg_likes * date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weighted_avgs,
                        SUM(date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weights_sum
                FROM date_weights_table)
            GROUP BY weekday),

        -- calculating likes number for the last 15 minutes interval
        likes_now AS
            (SELECT toDate(time) AS date,
                    toDayOfWeek(time) AS weekday,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    SUM(action = 'like') AS likes
            FROM simulator_20250120.feed_actions
            WHERE toStartOfInterval(time, toIntervalMinute(15)) = toStartOfInterval(now() - toIntervalMinute(15), toIntervalMinute(15))
            GROUP BY toDate(time), toDayOfWeek(time), toStartOfInterval(time, toIntervalMinute(15)))

        SELECT 'Number of User Likes' AS metric_name,
                time,
                likes / weighted_avg AS relative_deviation,
                lower_bound,
                upper_bound,
                avg_relative_deviation,
                (ROUND(avg_relative_deviation * weighted_avg)::int)::String AS avg_expected_value,
                likes::String AS metric_value,
                ROUND((likes * 100) / (avg_relative_deviation * weighted_avg) - 100, 2)::String AS change
        FROM likes_now JOIN weighted_avg_calculation USING(weekday)
        JOIN conf_int_table ON conf_int_table.time = likes_now.time_fifteen
        WHERE relative_deviation NOT BETWEEN lower_bound AND upper_bound
        """
        
        query_ctr = """
        WITH
        -- calculating users ctr every 15 minutes
        date_time_ctr AS 
            (SELECT toDate(time) AS date,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    SUM(action = 'like') / SUM(action = 'view') AS ctr
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) < today()
            GROUP BY toDate(time), toStartOfInterval(time, toIntervalMinute(15))),

        -- calculating average ctr throughout a day by 15 minutes interval for every day
        date_time_average_ctr AS 
            (SELECT date,
                    time_fifteen AS time,
                    ctr,
                    AVG(ctr) OVER (PARTITION BY date) AS avg_ctr
            FROM date_time_ctr),

        -- calculating the relative deviation of each ctr number
        -- from the average value related to its day
        relative_deviation_table AS 
            (SELECT date,
                    time,
                    ctr / avg_ctr AS relative_deviation
            FROM date_time_average_ctr),

        -- calculating the confidence interval of the relative
        -- deviations for each 15-minute interval
        conf_int_table AS 
            (SELECT time,
                    AVG(relative_deviation) - 2 * stddevSamp(relative_deviation) AS lower_bound,
                    AVG(relative_deviation) + 2 * stddevSamp(relative_deviation) AS upper_bound,
                    AVG(relative_deviation) AS avg_relative_deviation,
                    median(relative_deviation) AS median_relative_deviation
            FROM relative_deviation_table
            GROUP BY time),

        -- calculating the weights for every average ctr number 
        -- throughout a day by 15 minutes interval for every day
        date_weights_table AS 
            (SELECT date,
                    toDayOfWeek(date) AS weekday,
                    avg_ctr,
                    ROW_NUMBER() OVER (PARTITION BY toDayOfWeek(date) ORDER BY date) AS date_weight
            FROM date_time_average_ctr
            GROUP BY date,
                    toDayOfWeek(date) AS weekday,
                    avg_ctr),

        -- calculating the weighted average of every average ctr
        -- throughout a day by 15 minutes interval for every day for every weekday
        weighted_avg_calculation AS 
            (SELECT weekday,
                    MAX(weighted_avgs) / MAX(weights_sum) AS weighted_avg
            FROM
                (SELECT weekday,
                        SUM(avg_ctr * date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weighted_avgs,
                        SUM(date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weights_sum
                FROM date_weights_table)
            GROUP BY weekday),

        -- calculating ctr for the last 15 minutes interval
        ctr_now AS
            (SELECT toDate(time) AS date,
                    toDayOfWeek(time) AS weekday,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    SUM(action = 'like') / SUM(action = 'view') AS ctr
            FROM simulator_20250120.feed_actions
            WHERE toStartOfInterval(time, toIntervalMinute(15)) = toStartOfInterval(now() - toIntervalMinute(15), toIntervalMinute(15))
            GROUP BY toDate(time), toDayOfWeek(time), toStartOfInterval(time, toIntervalMinute(15)))

        SELECT 'User CTR' AS metric_name,
                time,
                ctr / weighted_avg AS relative_deviation,
                lower_bound,
                upper_bound,
                avg_relative_deviation,
                ROUND(avg_relative_deviation * weighted_avg, 3)::String AS avg_expected_value,
                ROUND(ctr, 3)::String AS metric_value,
                ROUND((ctr * 100) / (avg_relative_deviation * weighted_avg) - 100, 2)::String AS change
        FROM ctr_now JOIN weighted_avg_calculation USING(weekday)
        JOIN conf_int_table ON conf_int_table.time = ctr_now.time_fifteen
        WHERE relative_deviation NOT BETWEEN lower_bound AND upper_bound
        """
        
        query_sent_messages = """
        WITH
        -- calculating messages number every 15 minutes
        date_time_messages AS 
            (SELECT toDate(time) AS date,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    COUNT(1) AS messages
            FROM simulator_20250120.message_actions
            WHERE toDate(time) < today()
            GROUP BY toDate(time), toStartOfInterval(time, toIntervalMinute(15))),

        -- calculating average messages number throughout a day by 15 minutes interval for every day
        date_time_average_messages AS 
            (SELECT date,
                    time_fifteen AS time,
                    messages,
                    AVG(messages) OVER (PARTITION BY date) AS avg_messages
            FROM date_time_messages),

        -- calculating the relative deviation of each messages number
        -- from the average value related to its day
        relative_deviation_table AS 
            (SELECT date,
                    time,
                    messages / avg_messages AS relative_deviation
            FROM date_time_average_messages),

        -- calculating the confidence interval of the relative
        -- deviations for each 15-minute interval
        conf_int_table AS 
            (SELECT time,
                    AVG(relative_deviation) - 3 * stddevSamp(relative_deviation) AS lower_bound,
                    AVG(relative_deviation) + 3 * stddevSamp(relative_deviation) AS upper_bound,
                    AVG(relative_deviation) AS avg_relative_deviation,
                    median(relative_deviation) AS median_relative_deviation
            FROM relative_deviation_table
            GROUP BY time),

        -- calculating the weights for every average messages number 
        -- throughout a day by 15 minutes interval for every day
        date_weights_table AS 
            (SELECT date,
                    toDayOfWeek(date) AS weekday,
                    avg_messages,
                    ROW_NUMBER() OVER (PARTITION BY toDayOfWeek(date) ORDER BY date) AS date_weight
            FROM date_time_average_messages
            GROUP BY date,
                    toDayOfWeek(date) AS weekday,
                    avg_messages),

        -- calculating the weighted average of every average messages number
        -- throughout a day by 15 minutes interval for every day for every weekday
        weighted_avg_calculation AS 
            (SELECT weekday,
                    MAX(weighted_avgs) / MAX(weights_sum) AS weighted_avg
            FROM
                (SELECT weekday,
                        SUM(avg_messages * date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weighted_avgs,
                        SUM(date_weight) OVER (PARTITION BY weekday ORDER BY date) AS weights_sum
                FROM date_weights_table)
            GROUP BY weekday),

        -- calculating number of messages number for the last 15 minutes interval
        messages_now AS
            (SELECT toDate(time) AS date,
                    toDayOfWeek(time) AS weekday,
                    formatDateTime(toStartOfInterval(time, toIntervalMinute(15)), '%H:%M:%S') AS time_fifteen,
                    COUNT(1) AS messages
            FROM simulator_20250120.message_actions
            WHERE toStartOfInterval(time, toIntervalMinute(15)) = toStartOfInterval(now() - toIntervalMinute(15), toIntervalMinute(15))
            GROUP BY toDate(time), toDayOfWeek(time), toStartOfInterval(time, toIntervalMinute(15)))

        SELECT 'Number of Sent Messages' AS metric_name,
                time,
                messages / weighted_avg AS relative_deviation,
                lower_bound,
                upper_bound,
                avg_relative_deviation,
                (ROUND(avg_relative_deviation * weighted_avg)::int)::String AS avg_expected_value,
                messages::String AS metric_value,
                ROUND((messages * 100) / (avg_relative_deviation * weighted_avg) - 100, 2) AS change
        FROM messages_now JOIN weighted_avg_calculation USING(weekday)
        JOIN conf_int_table ON conf_int_table.time = messages_now.time_fifteen
        WHERE relative_deviation NOT BETWEEN lower_bound AND upper_bound
        """
        
        return pd.concat([ph.read_clickhouse(query=query_dau_feed, connection=connection),
                          ph.read_clickhouse(query=query_dau_messenger, connection=connection),
                          ph.read_clickhouse(query=query_views, connection=connection),
                          ph.read_clickhouse(query=query_likes, connection=connection),
                          ph.read_clickhouse(query=query_ctr, connection=connection),
                          ph.read_clickhouse(query=query_sent_messages, connection=connection)
                         ])
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def report_formation(df):
        dashboard_link = "http://superset.lab.karpov.courses/r/6292"
        if df.shape[0] == 0:
            message = "No anomalies have been detected"
        elif df.shape[0] == 1:
            data = df.loc[0]
            metric_name = data.metric_name
            start_time = data.time[:5]
            finish_time = (datetime.strptime(data.time, "%H:%M:%S") + timedelta(minutes=15)).strftime("%H:%M")
            metric_value = data.metric_value
            change = data.change
            avg_expected_value = data.avg_expected_value
            message = f"An anomaly has been detected in {metric_name} from <b>{start_time}</b> to <b>{finish_time}</b>. "\
            f"Current value is <b>{metric_value}</b>, deviating by <b>{change}</b>% from the average expected <b>{avg_expected_value}</b>.\n\n"\
            f'Click <a href="{dashboard_link}">the link</a> to view real-time metric changes.'\

        else:
            df['message'] = ""
            for metric_name in df.metric_name.values:
                data = df[df.metric_name == metric_name].iloc[0]
                metric_name = data.metric_name
                metric_value = data.metric_value
                change = data.change
                avg_expected_value = data.avg_expected_value

                df.loc[df.metric_name == metric_name, "message"] = f"- {metric_name}: current value is <b>{metric_value}</b>, "\
                                                            f"deviating by <b>{change}%</b> from the expected <b>{avg_expected_value}</b>\n"
            start_time = data.time[:5]
            finish_time = (datetime.strptime(data.time, "%H:%M:%S") + timedelta(minutes=15)).strftime("%H:%M")

            message = f"Anomalies have been detected in several metrics from <b>{start_time}</b> to <b>{finish_time}</b>:\n"\
                    f"{df['message'].sum()}\n"\
                    f'Click <a href="{dashboard_link}">the link</a> to view real-time metrics changes.'
        return message
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def report_sender(df, message):
        if df.shape[0] == 0:
            print(message)
        else:
            my_token = "**********************************************"
            bot = tg.Bot(token=my_token)
            chat_id = -*********
            bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
    
    connection = {
    'host': '*************************************',
    'password': '***************',
    'user': '*******',
    'database': '******************'
    }
    
    df = anomaly_detecter(connection)
    message = report_formation(df)
    report_sender(df, message)
    
anomaly_reporter = anomaly_reporter()