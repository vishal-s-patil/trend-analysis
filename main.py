from dotenv import load_dotenv
import os
from vertica import create_connection, read
from generate_graph import create_combined_graph
from send_mail import send_email_with_titles_and_images
from datetime import datetime, timedelta
load_dotenv()

mail_config = {
    "smtp_server": 'smtp.gmail.com',
    "smtp_port": 587,
    "sender_email": os.getenv('FROM_EMAIL'),
    "receiver_emails": os.getenv('TO_EMAILS', '').split(','),
    "password": os.getenv('GMAIL_APP_PASSWORD')
}

vertica_config = {
    "host": "vertica-cluster-url-02-prod-us.netcorein.com",
    "user": "devops",
    "password": os.getenv('VERTICA_SMARTECH_DEVOPS_PASSWORD'),
    "database": "smartech",
    "port": 5433,
    "autoCommit": False
}


def get_past_date(days_ago, start_date):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    past_date = start_date_obj - timedelta(days=days_ago)
    return past_date.strftime('%Y-%m-%d')


def plot_count_graph_day(args):
    title_image_pairs = []

    for opperation in args['opperations']:
        title = f"{opperation}"
        x_axis = "day"
        y_axis = f"count"

        (x, y, user_count_map) = get_day_wise_dimensions_count(opperation, args)
        
        img = create_combined_graph(x, y, user_count_map, title, x_axis, y_axis)
        title_image_pairs.append((title, img))
    
    return title_image_pairs, {'x':x, 'y':y, 'user_count_map':user_count_map}


def get_day_wise_dimensions_count(opperation, args):
    user_count_map = {}

    if args['days'] == 0:
        query = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            count(1)
            from netstats.trend_analysis 
            where date_trunc_day > '{args['from_datetime']}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{opperation[0]}'
            group by date_trunc_day 
            order by date_trunc_day;"""
    else:
        query = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            count(1)
            from netstats.trend_analysis 
            where date_trunc_day > '{get_past_date(args['days'], args['to_datetime'])}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{opperation[0]}'
            group by date_trunc_day 
            order by date_trunc_day;"""
    
    columns = ["date", "count"]

    df = read(args['vertica_connection'], query, columns)

    x = list(map(lambda ts: ts.day, df['date'].to_list()))
    x = list(map(lambda day: str(day), x))

    y = df["count"].to_list()

    return (x, y, user_count_map)


def get_day_wise_dimensions_performance(opperation, args):
    user_count_map = {}

    for user in args['users']:
        user_count_map[user] = [0] * 100

        if args['days'] == 0:
            query = f"""select
                date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
                avg(avg_duration_ms) as avg_duration_ms
                from netstats.trend_analysis 
                where date_trunc_day > '{args['from_datetime']}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{opperation[0]}'
                group by date_trunc_day 
                order by date_trunc_day;"""
        else:
            query = f"""select
                date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
                avg(avg_duration_ms) as avg_duration_ms
                from netstats.trend_analysis 
                where date_trunc_day > '{get_past_date(args['days'], args['to_datetime'])}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{opperation[0]}'
                group by date_trunc_day 
                order by date_trunc_day;"""
        if opperation == 'SELECT':
            for user in args['users']:
                if args['days'] == 0:
                    query_with_user = f"""select
                    date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
                    avg(avg_duration_ms) as avg_duration_ms
                    from netstats.trend_analysis 
                    where date_trunc_day > '{args['from_datetime']}' and date_trunc_day <= '{args['from_datetime']}' and operation = '{opperation[0]}' and user_name = '{user}'
                    group by date_trunc_day 
                    order by date_trunc_day;"""
                else:
                    query_with_user = f"""select
                    date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
                    avg(avg_duration_ms) as avg_duration_ms
                    from netstats.trend_analysis 
                    where date_trunc_day > '{get_past_date(args['days'], args['to_datetime'])}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{opperation[0]}' and user_name = '{user}'
                    group by date_trunc_day 
                    order by date_trunc_day;"""
                
                result = read(args['vertica_connection'], query_with_user, ["date", "count"])
                for i, cnt in enumerate(result['count'].to_list()):
                    user_count_map[user][i] = cnt

        columns = ["date", "count"]

        df = read(vertica_connection, query, columns)

        x = list(map(lambda ts: ts.day, df['date'].to_list()))
        x = list(map(lambda day: str(day), x))

        y = df["count"].to_list()

        return (x, y, user_count_map)


def plot_exec_time_graph_day(args):
    title_image_pairs = []

    for opperation in args['opperations']:
        title = f"{opperation}"
        x_axis = "day"
        y_axis = f"avg_duration_ms"

        (x, y, user_count_map) = get_day_wise_dimensions_performance(opperation)
        
        if opperation == 'SELECT':
            for user, user_list in user_count_map.items():
                if len(user_list) > len(x):
                    diff = len(user_list) - len(x)
                    while diff > 0:
                        user_list.pop()
                        diff -= 1
            img = create_combined_graph(x, y, user_count_map, title, x_axis, y_axis)
        else:
            user_count_map = {}
            img = create_combined_graph(x, y, user_count_map, title, x_axis, y_axis)
        title_image_pairs.append((title, img))
    
    return title_image_pairs, {'x':x, 'y':y, 'user_count_map':user_count_map}


def send_day_wise_graphs(vertica_connection):
    args = {
        'opperations': ['SELECT', 'COPY', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'],
        'users': ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': '2024-12-17',
        'days': 10,
    }

    title_image_pairs_count = plot_count_graph_day(args)
    title_image_pairs_performance = plot_exec_time_graph_day(args)

    title_image_pairs = []
    title_image_pairs.append(("Query Counts 4 Weeks Trend", title_image_pairs_count))
    title_image_pairs.append(("Query Execution Time 4 Weeks Trend", title_image_pairs_performance))
    items_per_row = 3

    mail_title = "Query count and performance of last 4 weeks"
    send_email_with_titles_and_images(title_image_pairs, mail_config, items_per_row, mail_title)


def send_week_wise_graphs(vertica_connection):
    number_of_weeks = 2
    to_date = '2024-11-17'

    args = {
        'opperations': ['SELECT', 'COPY', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'],
        'users': ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': to_date,
        'days': number_of_weeks*7,
    }

    for opperation in args['opperations']:
        _, day_wise_dimensions_count = get_day_wise_dimensions_count(opperation, args)
        _, day_wise_dimensions_performance = get_day_wise_dimensions_performance(opperation, args)

        for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
            if len(user_list) > len(day_wise_dimensions_performance['x']):
                diff = len(user_list) - len(day_wise_dimensions_performance['x'])
                while diff > 0:
                    user_list.pop()
                    diff -= 1
        print(day_wise_dimensions_performance)
        week_wise_dimensions_count = {
            'x': [],
            'y': [],
            'user_count_map': {}
        }
        week_wise_dimensions_performance = {
            'x': [],
            'y': [],
            'user_count_map': {user: [] for user in day_wise_dimensions_performance['user_count_map'].keys()}
        }

        print(day_wise_dimensions_performance['user_count_map'].items())
        print()

        for week in range(number_of_weeks):
            sum_count = 0
            sum_performance = 0
            for i in range(7):
                sum_count +=  day_wise_dimensions_count['y'][week*7 + i]
                sum_performance +=  day_wise_dimensions_performance['y'][week*7 + i]
            
            week_wise_dimensions_count['y'].append(sum_count/7)
            week_wise_dimensions_performance['y'].append(sum_performance/7)
            week_wise_dimensions_count['x'].append(week+1)
            week_wise_dimensions_performance['x'].append(week+1)
            

            for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
                sum_user = 0
                for i in range(7):
                    print(user_list[week*7 + i], end=' ')
                    sum_user += user_list[week*7 + i]
                print(user, sum_user)
                week_wise_dimensions_performance['user_count_map'][user].append(sum_user/7)
        
        print(week_wise_dimensions_count)
        print(week_wise_dimensions_performance)
        print()



def month_wise_grapgs(vertica_connection):
    pass


if __name__ == "__main__":
    vertica_connection = create_connection(vertica_config["host"], vertica_config["user"], vertica_config["password"], vertica_config["database"], vertica_config["port"], vertica_config["autoCommit"])

    # send_day_wise_graphs(vertica_connection)   
    send_week_wise_graphs(vertica_connection) 