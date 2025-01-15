import duckdb as ddb
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_kwarg_or_default(kwarg_name: str, kwargs: dict, default_value):
    if kwarg_name in kwargs:
        return kwargs[kwarg_name]
    else:
        return default_value


def check_required_kwargs(kwargs: dict, required_kwargs: list):
    for kwarg_name in required_kwargs:
        if kwarg_name not in kwargs:
            raise RuntimeError(f"Argument '{kwarg_name}' is not set")

def build_select_query(kwarg_name: str, kwargs: dict):
    column_list = get_kwarg_or_default(kwarg_name, kwargs, None)
    if column_list:
        column_count = len(column_list)
        query_start = "SELECT"
        if column_count == 1:
            query_start+=f" {column_list[0]}"
        else:
            count = 0
            for col in column_list:
                if count == 0:
                    query_start += f" {col}"
                    count += 1
                else:
                    query_start += f", {col} "
        return f"{query_start} FROM"
    else:
        raise RuntimeError('Required components for Query are not set')

def build_where_condition(kwarg_name: str, kwargs: dict):
    where_condition = get_kwarg_or_default(kwarg_name, kwargs, None)
    if where_condition:
        return f" WHERE {where_condition}"
    else:
        return None

def get_empty_dates(where:pd.DataFrame, scan_column:str, start:int, end:int) -> pd.DataFrame:
    """Return list of empty str dates which arent in dataframe
    
    :start - a value which is count of days minus today as start
    :end - a value which is count of days minus today as end
    """

    df_in = where
    base = datetime.today()- timedelta(days=end)
    date_list = [base - timedelta(days=x) for x in range(start+1)]
    calendar_in = pd.DataFrame(date_list, columns=['date'])
    calendar_out = ddb.query("select cast(date as date) as date from calendar_in").to_df()
    df = ddb.query(f'select distinct cast({scan_column} as date) as date from df_in').to_df()
    df_out = ddb.query(f"select cast(date as date) as date from calendar_out where date not in (select * from df)").to_df()
    list_dates = [str(value['date'].date()) for idx, value in df_out.iterrows()]

    if not list_dates:
        return None
    else:
        return list_dates

def get_period_range(period_type:str, current_date):
    current_date = current_date
    if period_type == 'month':
        date_start = (current_date.replace(day=1) - relativedelta(months=1))
        date_end = current_date.replace(day=1)
        return date_start, date_end

    elif period_type == 'quarter':
        current_quarter = (current_date.month - 1) // 3 + 1
        first_month_of_current_quarter = 3 * (current_quarter - 1) + 1
        date_start = (current_date.replace(month=first_month_of_current_quarter, day=1) - relativedelta(months=3))
        date_end = current_date.replace(month=first_month_of_current_quarter, day=1)
        return date_start, date_end

    elif period_type == 'year':
        date_start = current_date.replace(year=current_date.year - 1, month=1, day=1)
        date_end = current_date.replace(month=1, day=1)
        return date_start, date_end

    else:
        raise ValueError("Incorrect period")