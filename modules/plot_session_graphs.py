from modules.helpers import get_past_time
from modules.vertica import read
from modules.generate_graph import create_line_graph


def get_hour_wise_dimensions_session(args):
    if args['hours'] != 0:
        from_time = get_past_time(args['to_datetime'], args['hours'])
        query = f"""
                select date_trunc('min', snapshot_time::timestamp) as min_date_trunc, count(1)
                from netstats.sessions_full
                where snapshot_time >= '{from_time}' and statement_id is not null
                group by min_date_trunc
                order by min_date_trunc;
                """

        df = read(args['vertica_connection'], query, ['hour', 'count'])

        user_count_map = {}
        for user in args['users']:
            user_count_map[user] = [0] * 5000

        for user in args['users']:
            query_user = f"""
            select date_trunc('min', snapshot_time::timestamp) as min_date_trunc, count(1)
            from netstats.sessions_full
            where snapshot_time >= '{from_time}' and user_name = '{user}' and statement_id is not null
            group by min_date_trunc
            order by min_date_trunc;
            """

            df_user = read(args['vertica_connection'], query_user, ['hour', 'count'])
            for i, item in enumerate(df_user['count'].to_list()):
                user_count_map[user][i] = item

        x = list(map(lambda ts: str(ts.day) + ":" + str(ts.hour) + ":" + str(ts.minute), df['hour'].to_list()))
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


def plot_sessions_count_graph_hourly(vertica_connection):
    """
    sends hour_wise sessions count every day.
    """
    to_datetime = '2024-12-30 17:00'
    args = {
        'users': ['contact_summary', 'contact_summary_ds', 'sas', 'campaign_listing', 'campaign_report'],
        'vertica_connection': vertica_connection,
        'from_datetime': '2024-11-01',
        'to_datetime': to_datetime,
        'hours': 24,
    }

    title_image_pairs_sessions_count = []
    hour_wise_dimensions_session = get_hour_wise_dimensions_session(args)

    title = 'Minute wise sessions count'
    x_axis = 'hour'
    y_axis = 'count'

    img_session_hourly_count = create_line_graph(hour_wise_dimensions_session['x'],
                                                 hour_wise_dimensions_session['y'],
                                                 hour_wise_dimensions_session['user_count_map'], title, x_axis,
                                                 y_axis)
    title_image_pairs_sessions_count.append((title, img_session_hourly_count))

    return title_image_pairs_sessions_count
