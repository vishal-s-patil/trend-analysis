from modules.helpers import get_past_time
from modules.vertica import read


def get_hour_wise_dimensions_session(args):
    if args['hours'] != 0:
        from_time = get_past_time(args['to_datetime'], args['hours'])
        print(from_time)
        query = f"""
                select date_trunc('min', snapshot_time::timestamp) as min_date_trunc, count(1)
                from netstats.sessions_full
                where snapshot_time >= '{from_time}'
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
            where snapshot_time >= '{from_time}' and user_name = '{user}'
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
