from modules.helpers import get_past_time


def get_hour_wise_dimensions_session(args):
    if args['hours'] != 0:
        to_time = get_past_time(args['to_datetime'], args['hours'])
        print(to_time)
