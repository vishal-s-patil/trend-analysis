from modules.helpers import get_past_date


def get_day_wise_dimensions_system_metrix(args):
    from_date = get_past_date(1, args['to_datetime'])
    print(from_date)
