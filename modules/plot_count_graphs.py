from modules.generate_graph import create_combined_graph
from modules.vertica import read
from modules.helpers import get_past_date
from modules.helpers import fill_day_level_date


def plot_count_graph_day(args):
    title_image_pairs = []

    for operation in args['operations']:
        title = f"{operation} query counts"
        x_axis = "day"
        y_axis = f"count"

        dimensions_count = get_day_wise_dimensions_count(operation, args)

        img = create_combined_graph(dimensions_count['x'], dimensions_count['y'], dimensions_count['user_count_map'], title, x_axis, y_axis)
        title_image_pairs.append((title, img))
    
    return title_image_pairs


def get_day_wise_dimensions_count(operation, args):
    user_count_map = {}

    if args['days'] == 0:
        query = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            count(1)
            from netstats.trend_analysis 
            where date_trunc_day > '{args['from_datetime']}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{operation[0]}'
            group by date_trunc_day 
            order by date_trunc_day;"""
    else:
        query = f"""select
            date_trunc('day', date_trunc_time::timestamp) as date_trunc_day,
            count(1)
            from netstats.trend_analysis 
            where date_trunc_day > '{get_past_date(args['days'], args['to_datetime'])}' and date_trunc_day <= '{args['to_datetime']}' and operation = '{operation[0]}'
            group by date_trunc_day 
            order by date_trunc_day;"""
    
    columns = ["date", "count"]

    df = read(args['vertica_connection'], query, columns)
    df = fill_day_level_date(get_past_date(args['days']-1, args['to_datetime']), args['to_datetime'], df, "date")

    x = list(map(lambda ts: ts.day, df['date'].to_list()))
    x = list(map(lambda day: str(day), x))

    y = df["count"].to_list()

    return {'x': x, 'y': y, 'user_count_map': user_count_map}
