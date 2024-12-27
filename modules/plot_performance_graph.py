from .generate_graph import create_combined_graph
from modules.vertica import read
from .helpers import get_past_date

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

    df = read(args['vertica_connection'], query, columns)

    x = list(map(lambda ts: ts.day, df['date'].to_list()))
    x = list(map(lambda day: str(day), x))

    y = df["count"].to_list()

    return {'x':x, 'y':y, 'user_count_map':user_count_map}


def plot_exec_time_graph_day(args):
    title_image_pairs = []

    for opperation in args['opperations']:
        title = f"{opperation}"
        x_axis = "day"
        y_axis = f"avg_duration_ms"

        dimensions_performance = get_day_wise_dimensions_performance(opperation)
        
        if opperation == 'SELECT':
            for user, user_list in user_count_map.items():
                if len(user_list) > len(dimensions_performance['x']):
                    diff = len(user_list) - len(dimensions_performance['x'])
                    while diff > 0:
                        user_list.pop()
                        diff -= 1
            img = create_combined_graph(dimensions_performance['x'], dimensions_performance['y'], dimensions_performance['user_count_map'], title, x_axis, y_axis)
        else:
            user_count_map = {}
            img = create_combined_graph(dimensions_performance['x'], dimensions_performance['y'], dimensions_performance['user_count_map'], title, x_axis, y_axis)
        title_image_pairs.append((title, img))
    
    return title_image_pairs
