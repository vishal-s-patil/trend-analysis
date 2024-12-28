from modules.helpers import get_past_time
from modules.vertica import read


def get_hour_wise_dimensions_session(args):
    if args['hours'] != 0:
        from_time = get_past_time(args['to_datetime'], args['hours'])
        query = f"""
        select date_trunc('hour', snapshot_time::timestamp) as time, count(1)
        from netstats.sessions_full
        where snapshot_time > '{from_time}'
        group by time
        order by time;
        """

        df = read(args['vertica_connection'], query, ['hour', 'count'])

        user_count_map = {}
        for user in args['users']:
            query_user = f"""
            select date_trunc('hour', snapshot_time::timestamp) as time, count(1)
            from netstats.sessions_full
            where snapshot_time > '{from_time}' and user_name = '{user}'
            group by time
            order by time;
            """

            df_user = read(args['vertica_connection'], query_user, ['hour', 'count'])
            user_count_map[user] = df_user['count'].to_list()

        x = list(map(lambda ts: str(ts.day) + ":" + str(ts.hour), df['date'].to_list()))
        y = df['count'].to_list()

        return {'x': x, 'y': y, 'user_count_map': user_count_map}
