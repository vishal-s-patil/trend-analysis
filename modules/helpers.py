from datetime import datetime, timedelta
import pandas as pd


def get_past_date(days_ago, start_date):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    past_date = start_date_obj - timedelta(days=days_ago)
    return past_date.strftime('%Y-%m-%d')


def get_past_time(start_time, hours_ago):
    start_time_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
    past_time = start_time_obj - timedelta(hours=hours_ago)
    return past_time.strftime('%Y-%m-%d %H:%M')


def fill_min_level_date(start_datetime, end_datetime, df, date_field_column_name):
    """
    Fills missing minute-wise timestamps in the DataFrame and fills 0 or nil in other fields.
    """
    full_range = pd.date_range(start=start_datetime, end=end_datetime, freq='T')  # 'T' for minute frequency
    full_df = pd.DataFrame({date_field_column_name: full_range})
    df[date_field_column_name] = pd.to_datetime(df[date_field_column_name])
    result = pd.merge(full_df, df, on=date_field_column_name, how='left')
    result = result.fillna(0)  # Replace NaN with 0 in other columns
    return result


def fill_hour_level_date(start_datetime, end_datetime, df, date_field_column_name):
    """
    Fills missing hour-wise timestamps in the DataFrame and fills 0 or nil in other fields.
    """
    full_range = pd.date_range(start=start_datetime, end=end_datetime, freq='H')  # 'H' for hour frequency
    full_df = pd.DataFrame({date_field_column_name: full_range})
    df[date_field_column_name] = pd.to_datetime(df[date_field_column_name])
    result = pd.merge(full_df, df, on=date_field_column_name, how='left')
    result = result.fillna(0)
    return result


def fill_day_level_date(start_datetime, end_datetime, df, date_field_column_name):
    """
    Fills missing day-wise timestamps in the DataFrame and fills 0 or nil in other fields.
    """
    full_range = pd.date_range(start=start_datetime, end=end_datetime, freq='D')  # 'D' for day frequency
    full_df = pd.DataFrame({date_field_column_name: full_range})
    df[date_field_column_name] = pd.to_datetime(df[date_field_column_name])
    result = pd.merge(full_df, df, on=date_field_column_name, how='left')
    result = result.fillna(0)
    return result


def fill_month_level_date(start_datetime, end_datetime, df, date_field_column_name):
    """
    Fills missing month-wise timestamps in the DataFrame and fills 0 or nil in other fields.
    """
    full_range = pd.date_range(start=start_datetime, end=end_datetime, freq='MS')  # 'MS' for month start
    full_df = pd.DataFrame({date_field_column_name: full_range})
    df[date_field_column_name] = pd.to_datetime(df[date_field_column_name])
    result = pd.merge(full_df, df, on=date_field_column_name, how='left')
    result = result.fillna(0)
    return result
