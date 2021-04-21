import pandas as pd

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


def time(df_column):
    """This function convert string type column into datetime type"""

    return pd.to_datetime(df_column, format="%Y-%m-%d %H:%M:%S")


def add_zeros(df, df_column, leading: int, trailing: int):

    """ Add leading zeros and trailing zeros to a value from a column data frame"""

    # Add leading zeros to a string

    df[df_column] = df.apply(lambda x: x[df_column].zfill(leading), axis=1)

    # Add trailing zeros to a string

    df[df_column] = df.apply(lambda x: x[df_column].ljust(trailing, '0'), axis=1)

    return df[df_column]


def insert_colon(df, df_column, to_insert: str, step):

    """ Insert a string into a value column (also string) """

    df[df_column] = df.apply(lambda x: to_insert.join(x[df_column][i:i + 2] for i in range(0, len(x[df_column]), step)),
                             axis=1)

    return df[df_column]


def datetime_format(df, df_column, leading, trailing, insertion, step):

    """ Transform a string value from a a column dataframe into a datetime format 1422 to 14:22:00"""

    # Add leading and trailing zeros

    add_zeros(df, df_column, leading, trailing)

    # And insert colon in the date to fit the format

    insert_colon(df, df_column, insertion, step)

    return df[df_column]


def remove_blank_spaces(df, df_column, value: str):

    """Find blank spaces into a column and replace them by a value"""

    df.loc[df[df_column].str.contains(r'\s+') == True, ['DTHH']] = value


def fixing_datetime_column(df_list):

    print('Fixing datetime format...')

    for df in df_list:

        df.rename(columns={'IHOUR': 'DTHH'}, inplace=True)

        if 'DTHH' in df:

            if df['DTHH'].dtype == 'object':
                # It seems there are some rows with 'spaces' we need to feel to convert the column to integer

                remove_blank_spaces(df, 'DTHH', '0000')

            df['DTHH'].fillna(0, inplace=True)

            change_column_type(df, 'DTHH', 'int64')

            # Check that there are no times above 2400

            df.loc[df['DTHH'] >= 2400, ['DTHH']] = 0

            # Convert column to string

            change_column_type(df, 'DTHH', 'str')

            datetime_format(df, 'DTHH', 4, 6, ':', 2)

            df['LOCAL_DATETIME'] = df['IDATE'].astype('str').str.cat(df['DTHH'], sep=" ")

            df.drop(df[df['LOCAL_DATETIME'] == '1998-09-17 01:63:00'].index, inplace=True)

        if df['LOCAL_DATETIME'].dtype == 'object':

            df['LOCAL_DATETIME'] = df.apply(lambda x: time(x.LOCAL_DATETIME), axis=1)

            return df['LOCAL_DATETIME']



def change_column_type(df, df_column, new_type: str):

    df[df_column] = df[df_column].astype(new_type)

    return df[df_column]


def pre_processing(df_list):

    """Fix column date formats and assign values to columns with nan that will be used later"""

    print('Preparing data to preprocess...')

    preliminary_fill_nan(df_list)

    on_offshore(df_list, [2, 7])

    fixing_datetime_column(df_list)

    print('Preprocessing successfully done')

    return df_list


def nan_removal(df, percentage):

    """ Drop columns with high nan percentage"""

    nan_values = df.isna().sum()

    nan_percentage = nan_values / len(df) * 100

    filter_nan_percentage = nan_percentage > percentage

    high_nan_columns = df.columns[filter_nan_percentage].to_list()

    return df.drop(columns=high_nan_columns, inplace=True)


def renaming(df):

    """Rename dataframe columns"""

    df = df.rename(columns={'ACCTY': 'LOCATION_CITY_NAME',
                           'FACILITY_NAME': 'LOCATION_CITY_NAME',
                           'ACCITY': 'LOCATION_CITY_NAME',
                           'ACCNT': 'LOCATION_COUNTY_NAME',
                           'ACCOUNTY': 'LOCATION_COUNTY_NAME',
                           'ACCST': 'LOCATION_STATE_ABBREVIATION',
                           'ACSTATE': 'LOCATION_STATE_ABBREVIATION',
                           'FACILITY_STATE': 'LOCATION_STATE_ABBREVIATION',
                           'ACZIP': 'LOCATION_POSTAL_CODE',
                           'LATITUDE': 'LOCATION_LATITUDE',
                           'LONGITUDE': 'LOCATION_LONGITUDE',
                           'FACILITY_LATITUDE': 'LOCATION_LATITUDE',
                           'FACILITY_LONGITUDE': 'LOCATION_LONGITUDE',
                           'RPTID': 'REPORT_NUMBER',
                           'INADR': 'LOCATION_STREET_ADDRESS',
                           'CLASS': 'CLASS_LOCATION_TYPE',
                           'COMM': 'COMMODITY_RELEASED_TYPE',
                           'CSYS': 'SYSTEM_PART_INVOLVED',
                           'OFFSHORE': 'ON_OFF_SHORE',
                           'SHORE': 'ON_OFF_SHORE',
                           'OFFSHORE_TEXT': 'ON_OFF_SHORE',
                           'OPID': 'OPERATOR_ID',
                           'IFED': 'FEDERAL',
                           'INTER_INTRA': 'PIPE_FACILITY_TYPE',
                           'INTER_TEXT': 'PIPE_FACILITY_TYPE',
                           'INTER': 'PIPE_FACILITY_TYPE',
                           'TFAT': 'FATAL',
                           'EFAT': 'NUM_EMP_FATALITIES',
                           'FAT': 'FATAL',
                           'TINJ': 'INJURE',
                           'EINJ': 'NUM_EMP_INJURIES',
                           'INJ': 'INJURE',
                           'ACPRS': 'ACCIDENT_PSIG',
                           'INPRS': 'ACCIDENT_PSIG',
                           'INC_PRS': 'ACCIDENT_PSIG',
                           'MAOP': 'MOP_PSIG',
                           'MXPRS': 'MOP_PSIG',
                           'DSPRS': 'MOP_PSIG',
                           'PRTST': 'MOP_CFR_SECTION',
                           'TEST': 'EX_HYDROTEST_PRESSURE',
                           'PRTLK': 'CUSTOMER_TYPE',
                           'MLKD': 'MATERIAL_INVOLVED',
                           'MLKD_TEXT': 'MATERIAL_INVOLVED',
                           'NMDIA': 'PIPE_DIAMETER',
                           'NPS': 'PIPE_DIAMETER',
                           'THK': 'WT_STEEL',
                           'SPEC': 'PIPE_SPECIFICATION',
                           'PRTYR': 'INSTALLATION_YEAR',
                           'ITMYR': 'INSTALLATION_YEAR',
                           'MANYR': 'MANUFACTURED_YEAR',
                           'MANU': 'PIPE_MANUFACTURER',
                           'LOCLK': 'INCIDENT_AREA_TYPE',
                           'LOCLK_TEXT': 'INCIDENT_AREA_TYPE',
                           'PNAME': 'PREPARER_NAME',
                           'PHONE': 'PREPARER_PHONE',
                           'PPHONE': 'PREPARER_PHONE',
                           'PROT': 'UNDER_CATHODIC_PROTECTION_IND',
                           'FACAT': 'UNDER_CATHODIC_PROTECTION_IND',
                           'CAULK': 'CAUSE_DETAILS',
                           'ITYPE': 'RELEASE_TYPE',
                           'LRTYPE_TEXT': 'RELEASE_TYPE',
                           'ORGLK': 'ITEM_INVOLVED',
                           'PRTSY_TEXT': 'ITEM_INVOLVED',
                           'PRTSY': 'ITEM_INVOLVED',
                           'PRTFL': 'SYSTEM_PART_DETAILS',
                           'PRTFL_TEXT': 'SYSTEM_PART_DETAILS',
                           'LOSS': 'UNINTENTIONAL_RELEASE_BBLS',
                           'RECOV': 'RECOVERED_BBLS',
                           'FIRE': 'IGNITE_IND',
                           'IGNITE': 'IGNITE_IND',
                           'EXP': 'EXPLODE_IND',
                           'EXPLO': 'EXPLODE_IND',
                           'SMYS': 'PIPE_SMYS',
                           'CORRO': 'CORROSION_TYPE',
                           'UNINTENTIONAL_RELEASE': 'UNINTENTIONAL_RELEASE_BBLS'
                           }, inplace=True)

    return df

def nan_col_selection(df):

    """ Select column with nan values"""

    return df.columns[df.isna().any()].tolist()


def fillna_num_col(df, column_list):

    """ Fill nan values by column numeric type"""

    return df[column_list].select_dtypes(include=['float64', 'int64']).fillna(0)


def fillna_cat_col(df, column_list):

    """ Fill nan values by column object type"""

    return df[column_list].select_dtypes(exclude=['float64', 'int64']).fillna('NO DATA')


def df_clean(df, df_cat, df_num):

    """ Clean all nan values from dataframe"""

    for column in df_cat.columns:

        df[column] = df_cat[column]

    for column in df_num.columns:

        df[column] = df_num[column]

    return df


def cleaning_individual_df(df_list):

    """More cleaning of individual reports and selection of final variables to be used"""

    print('Selecting variables from individual dataframes')

    #hl_1986_to_2001

    df_list[0].drop(columns=['COOR', 'SPLOC', 'TELRN', 'ORGLO',
                             'CAUSO', 'NFAT', 'NINJ', 'CORR', 'PREVT',
                             'JNT', 'MOP_PSIG', 'DUR', 'CAULO', 'TMPMK',
                             'FACTD', 'ONECL', 'ONEOT', 'EXCAL'], errors='ignore', inplace=True)

    df_list[0]['PIPE_FACILITY_TYPE'].replace(['YES', 'NO'], ['INTERSTATE', 'INTRASTATE'], inplace=True)

    df_list[0]['ON_OFF_SHORE'].replace(['YES', 'NO'], ['OFFSHORE', 'ONSHORE'], inplace=True)

    #gtgg_1986_to_2001

    df_list[1].drop(columns=['MPOST', 'SURVY', 'OFFAREA', 'BNUMB',
                             'OFFST', 'OCS', 'OPJUD', 'STHH',
                             'STMN', 'TELRN', 'TELRT', 'MPEST', 'PRTFO',
                             'PRTSY', 'PRTSO', 'SEAM', 'LOCLO', 'DESCO',
                             'CAUCO', 'DMGO', 'NOTIF', 'MARK', 'MRKTP',
                             'CAULO', 'STAT', 'CTEST', 'MEDO', 'MLKDO'], errors='ignore', inplace=True)

    df_list[1]['ON_OFF_SHORE'].replace(['YES', 'NO'], ['OFFSHORE', 'ONSHORE'], inplace=True)

    df_list[1]['COMMODITY_RELEASED_TYPE'] = 'NATURAL GAS'

    #gd_1986_to_2004

    df_list[9].drop(columns=['OPJUD', 'STHH', 'STMN', 'TELRN', 'TELRT',
                             'MPEST', 'NOTIF', 'MARK', 'STAT'], errors='ignore', inplace=True)

    #hl_2002_to_2009

    df_list[3].drop(columns=['DOR', 'IYEAR', 'SPILLED', 'CLASS_TEXT', 'SPUNIT_TEXT',
                             'PEMAIL', 'NFAT', 'GPFAT', 'NINJ', 'GPINJ',
                             'IO_DRUG', 'IO_ALCO'], errors='ignore', inplace=True)

    #gtgg_2002_to_2009

    df_list[6].drop(columns=['DOR', 'IYEAR', 'OCS', 'HIGHCON',
                             'PEMAIL', 'NFAT', 'GPFAT', 'NINJ', 'GPINJ', 'EVAC',
                             'EVACNO', 'STHH', 'TELRN', 'TELDT',
                             'MAOPSEC1', 'MAOPSEC2', 'MAOPSEC3', 'MAOPSEC4',
                             'MAOPSECC', 'OVERPRS', 'PLAS_DUCT', 'PLAS_BRIT',
                             'PLAS_JNT', 'TYSYS_TEXT'
                             ], errors='ignore', inplace=True)

    #gd_2004_to_2009

    df_list[8].drop(columns=['FF', 'DOR', 'IYEAR', 'OCS', 'HIGHCON',
                             'PEMAIL', 'NFAT', 'GPFAT', 'NINJ', 'GPINJ', 'EVAC',
                             'EVACNO', 'STHH', 'STMN', 'TELRN', 'TELDT',
                             'MAOPEST', 'OVERPRS', 'PLAS_DUCT', 'PLAS_BRIT',
                             'PLAS_JNT', 'TYSYS_TEXT', 'NOTIF', 'MARKED', 'PERM_MARK',
                             'MKD_IN_TIME', 'PIPE_DAMAGE', 'PRS_TEST'
                             ], errors='ignore', inplace=True)

    #hl_2010_to_Present -> nothing to clean

    #gtgg_2010_to_Present

    df_list[7].drop(columns=['INCIDENT_AREA_SUBTYPE', 'CLASS_LOCATION_TYPE', 'PIR_RADIUS',
                             'HEAT_DAMAGE_IND', 'NON_HEAT_DAMAGE_IND',
                             'HCA_FATALITIES_IND', 'EST_COST_INTENT_REL',
                             'EST_COST_INTENT_REL_CURRENT', ], errors='ignore', inplace=True)

    # gd_2010_to_Present

    df_list[5].drop(columns=['FF', 'CLASS_LOCATION_TYPE', 'INCIDENT_AREA_SUBTYPE', 'EST_COST_UNINTENTIONAL_RELEASE',
                             'EST_COST_UNINTENT_REL_CURRENT', 'EST_COST_INTENT_REL_CURRENT', 'COMMERCIAL_AFFECTED',
                             'INDUSTRIAL_AFFECTED', 'RESIDENCES_AFFECTED'], errors='ignore', inplace=True)

    #LNG_2010_to_Present

    df_list[4].drop(columns=['UNINTENTIONAL_RELEASE_IND', 'INTENTIONAL_RELEASE_IND', 'EMERGENCY_SHUTDOWN_IND',
                             'RESULTED_FROM_OTHER_IND', 'NUM_OPER_AND_CONTRACTOR_EVAC',
                             'FACILITY_STATUS', 'FACILITY_LIQUID_VAPOR_RATE', 'FACILITY_NUM_VAPORIZERS',
                             'FACILITY_TOTAL_CAPACITY', 'FACILITY_SOURCE_LIQUEFY_IND',
                             'FACILITY_NUMBER_TANKS', 'FACILITY_VOLUME_STORAGE',
                             'EST_COST_INTENTIONAL_RELEASE', 'EST_COST_INTENT_REL_CURRENT',
                             'CCS_IN_PLACE_IND', 'CCS_OPERATING_IND', 'CCS_FUNCTIONAL_IND',
                             ], errors='ignore', inplace=True)

    return df_list


def concatenate_df(df_list):

    """Concatenate a list of dataframes"""

    print('Merging stuff...')

    return pd.concat(df_list , ignore_index=True)


def final_df(df, nan_values):

    """ Removing useless columns for visualization"""

    total_nan = df.isna().sum()

    filter_nan = total_nan > nan_values

    useless_columns = df.columns[filter_nan].to_list()

    return df.drop(columns=useless_columns)


def processing(df_list):

    """Clean data from all datasets in the directory. Filling nan values,
    renaming columns and removing useless columns"""

    print('Initiating data processing...')

    for df in df_list:

        renamed_df = renaming(df)

        nan_removal(renamed_df, 20)

        clean_df = df_clean(renamed_df, fillna_cat_col(renamed_df, nan_col_selection(renamed_df)),
                 fillna_num_col(renamed_df, nan_col_selection(renamed_df)))

        individual_clean_df = cleaning_individual_df(clean_df)

        mergedStuff = concatenate_df(individual_clean_df)

        merged_df_datavis = final_df(mergedStuff, 6500)

        return merged_df_datavis

    print('Data processing successfully done')
