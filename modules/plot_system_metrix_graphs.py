from modules.helpers import get_past_date
from modules.vertica import read


def get_day_wise_dimensions_avg_mem_usage(args):
    if args['hours'] != 0:
        from_date = get_past_date(1, args['to_datetime'])
        query = f"""
                select
                    node_name,
                    avg(average_memory_usage_percent) as day_average_memory_usage_percent
                from system_resource_usage
                where end_time > '{from_date}'
                group by node_name
                order by node_name;
                """

        df = read(args['vertica_connection'], query, ['hour', 'count'])

        user_count_map = {}
        # for user in args['users']:
        #     user_count_map[user] = [0] * 100
        #
        # for user in args['users']:
        #     query_user = f"""
        #             select date_trunc('hour', snapshot_time::timestamp) as time, avg(cnt)
        #             from (
        #                select date_trunc('min', snapshot_time::timestamp) as snapshot_time, count(1) as cnt
        #                  from netstats.sessions_full
        #                  where snapshot_time > '{from_time}' and user_name = '{user}'
        #                  group by snapshot_time
        #             ) as x
        #             group by time
        #             order by time;
        #             """
        #
        #     df_user = read(args['vertica_connection'], query_user, ['hour', 'count'])
        #     for i, item in enumerate(df_user['count'].to_list()):
        #         user_count_map[user][i] = item

        x = list(map(lambda ts: str(ts.hour), df['hour'].to_list()))
        y = df['count'].to_list()

        day_wise_dimensions_mem_usage = {
            'x': x,
            'y': y,
            'user_count_map': user_count_map
        }

        for user, user_list in day_wise_dimensions_mem_usage['user_count_map'].items():
            if len(user_list) > len(day_wise_dimensions_mem_usage['x']):
                diff = len(user_list) - len(day_wise_dimensions_mem_usage['x'])
                while diff > 0:
                    user_list.pop()
                    diff -= 1

        return day_wise_dimensions_mem_usage


def get_day_wise_dimensions_avg_cpu_usage(args):
    if args['hours'] != 0:
        from_date = get_past_date(1, args['to_datetime'])
        query = f"""
                select
                    node_name,
                    avg(average_cpu_usage_percent) as day_average_cpu_usage_percent
                from system_resource_usage
                where end_time > '{from_date}'
                group by node_name
                order by node_name;
                """

        df = read(args['vertica_connection'], query, ['hour', 'count'])

        user_count_map = {}
        # for user in args['users']:
        #     user_count_map[user] = [0] * 100
        #
        # for user in args['users']:
        #     query_user = f"""
        #             select date_trunc('hour', snapshot_time::timestamp) as time, avg(cnt)
        #             from (
        #                select date_trunc('min', snapshot_time::timestamp) as snapshot_time, count(1) as cnt
        #                  from netstats.sessions_full
        #                  where snapshot_time > '{from_time}' and user_name = '{user}'
        #                  group by snapshot_time
        #             ) as x
        #             group by time
        #             order by time;
        #             """
        #
        #     df_user = read(args['vertica_connection'], query_user, ['hour', 'count'])
        #     for i, item in enumerate(df_user['count'].to_list()):
        #         user_count_map[user][i] = item

        x = list(map(lambda ts: str(ts.hour), df['hour'].to_list()))
        y = df['count'].to_list()

        day_wise_dimensions_cpu_usage = {
            'x': x,
            'y': y,
            'user_count_map': user_count_map
        }

        for user, user_list in day_wise_dimensions_cpu_usage['user_count_map'].items():
            if len(user_list) > len(day_wise_dimensions_cpu_usage['x']):
                diff = len(user_list) - len(day_wise_dimensions_cpu_usage['x'])
                while diff > 0:
                    user_list.pop()
                    diff -= 1

        return day_wise_dimensions_cpu_usage


def get_day_wise_dimensions_system_metrix(args):
    day_wise_dimensions_mem_usage = get_day_wise_dimensions_avg_mem_usage(args)
    day_wise_dimensions_cpu_usage = get_day_wise_dimensions_avg_cpu_usage(args)

    return day_wise_dimensions_mem_usage, day_wise_dimensions_cpu_usage
