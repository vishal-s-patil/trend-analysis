from dotenv import load_dotenv
import os

from modules.plot_system_metrix_graphs import get_day_wise_dimensions_system_metrix
from modules.vertica import create_connection
from modules.plot_count_graphs import plot_count_graph_day, get_day_wise_dimensions_count
from modules.plot_performance_graph import plot_exec_time_graph_day, get_day_wise_dimensions_performance
from modules.send_mail import send_email_with_titles_and_images
from modules.generate_graph import create_combined_graph, create_line_graph
from modules.plot_session_graphs import get_hour_wise_dimensions_session
from modules.plot_queue_graphs import get_hour_wise_dimensions_queue

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


def plot_queues_count_graph_hourly(vertica_connection):
    to_datetime = '2024-12-28 19:00'
    args = {
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': to_datetime,
        'hours': 24,
    }

    title_image_pairs_queues_count = []
    hour_wise_dimensions_queue = get_hour_wise_dimensions_queue(args)

    title = 'hourly queues count'
    x_axis = 'hour'
    y_axis = 'count'

    img_queue_hourly_count = create_combined_graph(hour_wise_dimensions_queue['x'],
                                                     hour_wise_dimensions_queue['y'],
                                                     hour_wise_dimensions_queue['user_count_map'], title, x_axis,
                                                     y_axis)
    title_image_pairs_queues_count.append((title, img_queue_hourly_count))

    return title_image_pairs_queues_count


def send_day_wise_graphs(vertica_connection):
    args = {
        'operations': ['SELECT', 'COPY', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'],
        'users': ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': '2024-12-17',
        'days': 10,
    }

    # title_image_pairs_count = plot_count_graph_day(args)
    # title_image_pairs_performance = plot_exec_time_graph_day(args)
    title_image_pairs_sessions_count = plot_sessions_count_graph_hourly(vertica_connection)
    title_image_pairs_queues_count = plot_queues_count_graph_hourly(vertica_connection)

    title_image_pairs = [
                        # ("Query Counts 4 Weeks Trend", title_image_pairs_count),
                        #  ("Query Execution Time 4 Weeks Trend", title_image_pairs_performance),
                         ("Hourly sessions count", title_image_pairs_sessions_count),
                         ("Hourly queues count", title_image_pairs_queues_count)]

    items_per_row = 3
    mail_title = "Query count and performance of last 4 weeks"
    send_email_with_titles_and_images(title_image_pairs, mail_config, items_per_row, mail_title)


def send_week_wise_graphs(vertica_connection):
    number_of_weeks = 4
    to_date = '2024-11-27'

    args = {
        'operations': ['SELECT', 'COPY', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'],
        'users': ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': to_date,
        'days': number_of_weeks * 7,
    }

    x_axis = "week"

    title_image_pairs_count = []
    title_image_pairs_performance = []

    for operation in args['operations']:
        title = f"{operation}"
        day_wise_dimensions_count = get_day_wise_dimensions_count(operation, args)
        day_wise_dimensions_performance = get_day_wise_dimensions_performance(operation, args)

        for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
            if len(user_list) > len(day_wise_dimensions_performance['x']):
                diff = len(user_list) - len(day_wise_dimensions_performance['x'])
                while diff > 0:
                    user_list.pop()
                    diff -= 1

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

        for week in range(number_of_weeks):
            sum_count = 0
            sum_performance = 0
            for i in range(7):
                sum_count += day_wise_dimensions_count['y'][week * 7 + i]
                sum_performance += day_wise_dimensions_performance['y'][week * 7 + i]

            week_wise_dimensions_count['y'].append(sum_count / 7)
            week_wise_dimensions_performance['y'].append(sum_performance / 7)
            week_wise_dimensions_count['x'].append(week + 1)
            week_wise_dimensions_performance['x'].append(week + 1)

            for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
                sum_user = 0
                for i in range(7):
                    sum_user += user_list[week * 7 + i]
                week_wise_dimensions_performance['user_count_map'][user].append(sum_user / 7)

        img_count = create_combined_graph(week_wise_dimensions_count['x'], week_wise_dimensions_count['y'], {}, title,
                                          x_axis, "count")
        if operation == 'SELECT':
            img_performance = create_combined_graph(week_wise_dimensions_performance['x'],
                                                    week_wise_dimensions_performance['y'],
                                                    week_wise_dimensions_performance['user_count_map'], title, x_axis,
                                                    "avg_duration_ms")
        else:
            img_performance = create_combined_graph(week_wise_dimensions_performance['x'],
                                                    week_wise_dimensions_performance['y'], {}, title, x_axis,
                                                    "avg_duration_ms")

        title_image_pairs_count.append((title, img_count))
        title_image_pairs_performance.append((title, img_performance))

    title_image_pairs = []
    title_image_pairs.append((f"Query Counts {number_of_weeks} Weeks Trend", title_image_pairs_count))
    title_image_pairs.append((f"Query Execution Time {number_of_weeks} Weeks Trend", title_image_pairs_performance))
    items_per_row = 3

    mail_title = "Query count and performance of last 4 weeks."
    send_email_with_titles_and_images(title_image_pairs, mail_config, items_per_row, mail_title)


def send_month_wise_graphs(vertica_connection):
    number_of_months = 2
    to_date = '2024-12-17'

    args = {
        'operations': ['SELECT', 'COPY', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'],
        'users': ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': to_date,
        'days': number_of_months * 30,
    }

    x_axis = "month"

    title_image_pairs_count = []
    title_image_pairs_performance = []

    for operation in args['operations']:
        title = f"{operation}"
        day_wise_dimensions_count = get_day_wise_dimensions_count(operation, args)
        day_wise_dimensions_performance = get_day_wise_dimensions_performance(operation, args)

        for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
            if len(user_list) > len(day_wise_dimensions_performance['x']):
                diff = len(user_list) - len(day_wise_dimensions_performance['x'])
                while diff > 0:
                    user_list.pop()
                    diff -= 1

        month_wise_dimensions_count = {
            'x': [],
            'y': [],
            'user_count_map': {}
        }
        month_wise_dimensions_performance = {
            'x': [],
            'y': [],
            'user_count_map': {user: [] for user in day_wise_dimensions_performance['user_count_map'].keys()}
        }

        for month in range(number_of_months):
            sum_count = 0
            sum_performance = 0
            for i in range(30):
                sum_count += day_wise_dimensions_count['y'][month * 30 + i]
                sum_performance += day_wise_dimensions_performance['y'][month * 30 + i]

            month_wise_dimensions_count['y'].append(sum_count / 30)
            month_wise_dimensions_performance['y'].append(sum_performance / 30)
            month_wise_dimensions_count['x'].append(month + 1)
            month_wise_dimensions_performance['x'].append(month + 1)

            for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
                sum_user = 0
                for i in range(7):
                    sum_user += user_list[month * 30 + i]
                month_wise_dimensions_performance['user_count_map'][user].append(sum_user / 30)

        img_count = create_combined_graph(month_wise_dimensions_count['x'], month_wise_dimensions_count['y'], {}, title,
                                          x_axis, "count")
        if operation == 'SELECT':
            img_performance = create_combined_graph(month_wise_dimensions_performance['x'],
                                                    month_wise_dimensions_performance['y'],
                                                    month_wise_dimensions_performance['user_count_map'], title, x_axis,
                                                    "avg_duration_ms")
        else:
            img_performance = create_combined_graph(month_wise_dimensions_performance['x'],
                                                    month_wise_dimensions_performance['y'], {}, title, x_axis,
                                                    "avg_duration_ms")

        title_image_pairs_count.append((title, img_count))
        title_image_pairs_performance.append((title, img_performance))

    title_image_pairs = []
    title_image_pairs.append((f"Query Counts {number_of_months} Weeks Trend", title_image_pairs_count))
    title_image_pairs.append((f"Query Execution Time {number_of_months} Weeks Trend", title_image_pairs_performance))
    items_per_row = 3

    mail_title = "Query count and performance of last 2 months."
    send_email_with_titles_and_images(title_image_pairs, mail_config, items_per_row, mail_title)


def plot_sessions_count_graph_hourly(vertica_connection):
    """
    sends hour_wise sessions count every day.
    """
    to_datetime = '2024-12-28 17:00'
    args = {
        'users': ['contact_summary', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': to_datetime,
        'hours': 24,
    }

    title_image_pairs_sessions_count = []
    hour_wise_dimensions_session = get_hour_wise_dimensions_session(args)

    title = 'hourly sessions count'
    x_axis = 'hour'
    y_axis = 'count'

    img_session_hourly_count = create_line_graph(hour_wise_dimensions_session['x'],
                                                     hour_wise_dimensions_session['y'],
                                                     hour_wise_dimensions_session['user_count_map'], title, x_axis,
                                                     y_axis)
    title_image_pairs_sessions_count.append((title, img_session_hourly_count))

    return title_image_pairs_sessions_count

if __name__ == "__main__":
    vertica_connection = create_connection(vertica_config["host"], vertica_config["user"], vertica_config["password"],
                                           vertica_config["database"], vertica_config["port"],
                                           vertica_config["autoCommit"])

    send_day_wise_graphs(vertica_connection)
