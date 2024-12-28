from datetime import datetime, timedelta


def get_past_date(days_ago, start_date):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    past_date = start_date_obj - timedelta(days=days_ago)
    return past_date.strftime('%Y-%m-%d')


def get_past_time(start_time, hours_ago):
    start_time_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
    past_time = start_time_obj - timedelta(hours=hours_ago)
    return past_time.strftime('%Y-%m-%d %H:%M')
