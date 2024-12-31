from modules.helpers import get_past_time
from modules.vertica import read
from modules.generate_graph import create_line_graph
from modules.helpers import fill_min_level_date
from datetime import datetime


def get_hour_wise_dimensions_queue(args):
    if args['hours'] != 0:
        from_time = get_past_time(args['to_datetime'], args['hours'])
        query = f"""
        select date_trunc('min', queue_entry_timestamp::timestamp) as time, count(1)
        from netstats.resource_queues_full
        where queue_entry_timestamp >= '{from_time}'
        group by time
        order by time
        """

        df = read(args['vertica_connection'], query, ['hour', 'count'])
        df = fill_min_level_date(from_time, datetime.now(), df, 'hour')

        user_count_map = {}
        for user in args['pools']:
            user_count_map[user] = [0] * 5000

        for user in args['pools']:
            query_user = f"""
            select date_trunc('min', queue_entry_timestamp::timestamp) as time, count(1)
            from netstats.resource_queues_full
            where queue_entry_timestamp >= '{from_time}' and pool_name = '{user}'
            group by time
            order by time
            """

            df_user = read(args['vertica_connection'], query_user, ['hour', 'count'])
            for i, item in enumerate(df_user['count'].to_list()):
                user_count_map[user][i] = item

        x = list(map(lambda ts: str(ts.hour) + ":" + str(ts.minute), df['hour'].to_list()))
        y = df['count'].to_list()

        day_wise_dimensions_performance = {
            'x': x,
            'y': y,
            'user_count_map': user_count_map
        }

        for user, user_list in day_wise_dimensions_performance['user_count_map'].items():
            if len(user_list) > len(day_wise_dimensions_performance['x']):
                diff = len(user_list) - len(day_wise_dimensions_performance['x'])
                while diff > 0:
                    user_list.pop()
                    diff -= 1

        return day_wise_dimensions_performance


def plot_queues_count_graph_hourly(vertica_connection, to_datetime):
    args = {
        'vertica_connection': vertica_connection,
        'pools': ['contact_summary_pool', 'sas_pool', 'campaign_listing_pool', 'campaign_report_pool'],
        'from_datetime': '2024-11-01',
        'to_datetime': to_datetime,
        'hours': 24,
    }

    title_image_pairs_queues_count = []
    hour_wise_dimensions_queue = get_hour_wise_dimensions_queue(args)

    title = 'minute wise queue count'
    x_axis = 'hour'
    y_axis = 'count'

    img_queue_hourly_count = create_line_graph(hour_wise_dimensions_queue['x'],
                                               hour_wise_dimensions_queue['y'],
                                               hour_wise_dimensions_queue['user_count_map'], title, x_axis,
                                               y_axis)
    title_image_pairs_queues_count.append((title, img_queue_hourly_count))

    return title_image_pairs_queues_count
