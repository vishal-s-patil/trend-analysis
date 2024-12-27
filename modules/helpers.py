from datetime import datetime, timedelta

def get_past_date(days_ago, start_date):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    past_date = start_date_obj - timedelta(days=days_ago)
    return past_date.strftime('%Y-%m-%d')
