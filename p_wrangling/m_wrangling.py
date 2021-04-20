
columns_to_fill_zeros = ['FATAL', 'INJURE', 'INC_PRES']

columns_to_fill_empty = ['ONSHORE_CITY_NAME', 'OFF_ACCIDENT_ORIGIN', 'ONSHORE_COUNTY_NAME',
                         'OFFSHORE_COUNTY_NAME', 'ONSHORE_STATE_ABBREVIATION', 'OFFSHORE_STATE_ABBREVIATION']

def on_offshore(df_list, index):

    """ Fix locations of datesets hl2010toPresent and gtggungs2010toPresent """

    print('Classifying onshore and offshore incidents...')

    for i in index:

        df_list[i]['LOCATION_CITY_NAME'] = df_list[i].apply(lambda x:
                                                                x.ONSHORE_CITY_NAME +
                                                                x.OFF_ACCIDENT_ORIGIN, axis=1)
        df_list[i]['LOCATION_COUNTY_NAME'] = df_list[i].apply(lambda x:
                                                                  x.ONSHORE_COUNTY_NAME +
                                                                  x.OFFSHORE_COUNTY_NAME, axis=1)
        df_list[i]['LOCATION_STATE_ABBREVIATION'] = df_list[i].apply(lambda x:
                                                                         x.ONSHORE_STATE_ABBREVIATION +
                                                                         x.OFFSHORE_STATE_ABBREVIATION, axis=1)


def preliminary_fill_nan(df_list):

    """Check nan values in dataframes and fill them with empty and zero values"""

    print(f'Filling preliminary nan values...')

    for df in df_list:

        for column in columns_to_fill_empty:

            fillna_empty(df, column)

        for column in columns_to_fill_zeros:

            fillna_zeros(df, column)


def fillna_empty(df, column):

    """Fill column nan values with empty values"""

    if column in df:

        return df[column].fillna('', inplace=True)


def fillna_zeros(df, column):

    """Fill column nan values with zeros"""

    if column in df:

        return df[column].fillna(0, inplace=True)


def processing(df_list):

    """Clean data from all datasets in the directory. From filling nan values,
    fixing dates, remove useless columns, etc."""

    preliminary_fill_nan(df_list)

    on_offshore(df_list, [2, 7])