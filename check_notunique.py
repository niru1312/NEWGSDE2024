"""
This module contains common util functions to check not unique records
"""

def check_notunique(df,column_name):
    num_unique_values_A = df[column_name].nunique()
    if num_unique_values_A == len(df[column_name]):
        print("Column", column_name, "has unique values")
    else:
        print("Column", column_name, "has duplicate values")
    return None