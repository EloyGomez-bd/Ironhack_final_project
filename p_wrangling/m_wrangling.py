import pandas as pd

columns_to_fill_zeros = ['FATAL', 'INJURE', 'INC_PRES']

columns_to_fill_empty = ['ONSHORE_CITY_NAME', 'OFF_ACCIDENT_ORIGIN', 'ONSHORE_COUNTY_NAME',
                         'ONSHORE_STATE_ABBREVIATION', 'OFFSHORE_STATE_ABBREVIATION']

def on_offshore(df_list, column_list):

    list_index = [2, 7]

    for i in list_index:

        fillna_empty(column_list)

        df_list[i]['LOCATION_CITY_NAME'] = df_list[i].apply(lambda x:
                                                     x.ONSHORE_CITY_NAME +
                                                     x.OFF_ACCIDENT_ORIGIN, axis=1)
        df_list[i]['LOCATION_COUNTY_NAME'] = df_list[i].apply(lambda x:
                                                     x.ONSHORE_COUNTY_NAME +
                                                     x.OFFSHORE_COUNTY_NAME, axis=1)
        df_list[i]['LOCATION_STATE_ABBREVIATION'] = df_list[i].apply(lambda x:
                                                     x.ONSHORE_STATE_ABBREVIATION +
                                                     x.OFFSHORE_STATE_ABBREVIATION, axis=1)


def fillna_empty(df_list, column_list):

    """Fill column nan values with empty values"""

    for df in df_list:

        for column in column_list:

            if column in df:

                return df[column].fillna('', inplace=True)


def fillna_zeros(df_list, column_list):

    """Fill column nan values with zeros"""

    for df in df_list:

        for column in column_list:

            if column in df:

                return df[column].fillna(0, inplace=True)


def processing(df_list):

    """Clean data from all datasets in the directory. From filling nan values,
    fixing dates, remove useless columns, etc."""

    fillna_zeros(df_list, columns_to_fill_zeros)

    on_offshore(df_list, columns_to_fill_empty)