from dotenv import load_dotenv
import os
from vertica import create_connection, read
from generate_graph import create_combined_graph
from send_mail import send_email_with_titles_and_images
from datetime import datetime, timedelta
load_dotenv()

# mail config 
mail_config = {
    "smtp_server": 'smtp.gmail.com',
    "smtp_port": 587,
    "sender_email": os.getenv('FROM_EMAIL'),
    "receiver_emails": os.getenv('TO_EMAILS', '').split(','),
    "password": os.getenv('GMAIL_APP_PASSWORD')
}

number_of_days = 24

vertica_config = {
    "host": "vertica-cluster-url-02-prod-us.netcorein.com",
    "user": "devops",
    "password": os.getenv('VERTICA_SMARTECH_DEVOPS_PASSWORD'),
    "database": "smartech",
    "port": 5433,
    "autoCommit": False
}

def get_past_date(days_ago):
    today = datetime.today()
    past_date = today - timedelta(days=days_ago)
    return past_date.strftime('%Y-%m-%d')


def plot_count_graph(vertica_connection, opperations, users):
    user_count_map = {}
    title_image_pairs = []

    for user in users:
        user_count_map[user] = [0] * 24

    for opperation in opperations:
        query = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            count(1)
            from netstats.trend_analysis 
            where date_trunc_day >= '{get_past_date(number_of_days)}' and operation = '{opperation[0]}'
            group by date_trunc_day 
            order by date_trunc_day;"""
        
        for user in users:
            query_with_user = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            count(1)
            from netstats.trend_analysis 
            where date_trunc_day >= '{get_past_date(number_of_days)}' and operation = '{opperation[0]}' and user_name = '{user}'
            group by date_trunc_day 
            order by date_trunc_day;"""
            result = read(vertica_connection, query_with_user, ["date", "count"])
            for i, cnt in enumerate(result['count'].to_list()):
                user_count_map[user][i] = cnt

        
        columns = ["date", "count"]

        df = read(vertica_connection, query, columns)

        title = f"All {opperation} day wise trend for 4 weeks"
        x_axis = "day"
        y_axis = f"count"

        x = list(map(lambda ts: ts.day, df['date'].to_list()))
        x = list(map(lambda day: str(day), x))
        img = create_combined_graph(x, df["count"].to_list(), user_count_map, title, x_axis, y_axis)
        title_image_pairs.append((title, img))

        for user in users:
            user_count_map[user] = [0] * 24
    
    send_email_with_titles_and_images(title_image_pairs, mail_config, "query count of last 4 weeks")

def plot_exec_time_graph(vertica_connection, opperations, users):
    user_count_map = {}
    title_image_pairs = []

    for user in users:
        user_count_map[user] = [0] * 24

    for opperation in opperations:
        query = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            avg(avg_duration_ms) as avg_duration_ms
            from netstats.trend_analysis 
            where date_trunc_day >= '2024-11-30' and operation = '{opperation[0]}'
            group by date_trunc_day 
            order by date_trunc_day;"""
        
        for user in users:
            query_with_user = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            avg(avg_duration_ms) as avg_duration_ms
            from netstats.trend_analysis 
            where date_trunc_day >= '2024-11-30' and operation = '{opperation[0]}' and user_name = '{user}'
            group by date_trunc_day 
            order by date_trunc_day;"""
            result = read(vertica_connection, query_with_user, ["date", "count"])
            for i, cnt in enumerate(result['count'].to_list()):
                user_count_map[user][i] = cnt

        
        columns = ["date", "count"]

        df = read(vertica_connection, query, columns)

        title = f"All {opperation} day wise trend for 4 weeks"
        x_axis = "day"
        y_axis = f"avg_duration_ms"

        x = list(map(lambda ts: ts.day, df['date'].to_list()))
        x = list(map(lambda day: str(day), x))
        img = create_combined_graph(x, df["count"].to_list(), user_count_map, title, x_axis, y_axis)
        title_image_pairs.append((title, img))

        for user in users:
            user_count_map[user] = [0] * 24
    
    send_email_with_titles_and_images(title_image_pairs, mail_config, "query performance of last 4 weeks")

if __name__ == "__main__":
    vertica_connection = create_connection(vertica_config["host"], vertica_config["user"], vertica_config["password"], vertica_config["database"], vertica_config["port"], vertica_config["autoCommit"])

    opperations = ['SELECT', 'COPY', 'INSERT', 'UPDATE', 'DELETE', 'MERGE']
    users = ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'] # 'sbuilder' #['behaviour', 'campaign_listing', 'contact_summary', 'raman', 'sbuilder', 'vwriter'] 

    plot_exec_time_graph(vertica_connection, opperations, users)