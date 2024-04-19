"""
This module contains common util functions to check not null records
"""

def check_notnull(df):
    rows_with_null = df[df.isnull().any(axis=1)]
    if len(rows_with_null) == 0:
        print('Dataframe is NOT NULL')
    else:
        print('Dataframe is NULL')
    return None