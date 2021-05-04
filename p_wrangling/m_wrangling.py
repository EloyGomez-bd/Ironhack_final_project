import pandas as pd
import requests
from bs4 import BeautifulSoup

columns_to_fill_zeros = ['FATAL', 'INJURE', 'INC_PRES']

columns_to_fill_empty = ['ONSHORE_CITY_NAME', 'OFF_ACCIDENT_ORIGIN', 'ONSHORE_COUNTY_NAME',
                         'OFFSHORE_COUNTY_NAME', 'ONSHORE_STATE_ABBREVIATION', 'OFFSHORE_STATE_ABBREVIATION']

url = 'https://www.infoplease.com/us/postal-information/state-abbreviations-and-state-postal-codes'

url_mileage = 'https://www.ncsl.org/research/energy/state-gas-pipelines.aspx'

export_path = './data/processed/pipelines_incident.csv'

analysis_path = './data/processed/pipelines_incident_for_modelling.csv'

columns_to_rename = {'ACCTY': 'LOCATION_CITY_NAME', 'FACILITY_NAME': 'LOCATION_CITY_NAME',
                       'ACCITY': 'LOCATION_CITY_NAME', 'ACCNT': 'LOCATION_COUNTY_NAME',
                       'ACCOUNTY': 'LOCATION_COUNTY_NAME', 'ACCST': 'LOCATION_STATE_ABBREVIATION',
                       'ACSTATE': 'LOCATION_STATE_ABBREVIATION',
                       'FACILITY_STATE': 'LOCATION_STATE_ABBREVIATION', 'ACZIP': 'LOCATION_POSTAL_CODE',
                       'LATITUDE': 'LOCATION_LATITUDE', 'LONGITUDE': 'LOCATION_LONGITUDE',
                       'FACILITY_LATITUDE': 'LOCATION_LATITUDE', 'FACILITY_LONGITUDE': 'LOCATION_LONGITUDE',
                       'RPTID': 'REPORT_NUMBER', 'INADR': 'LOCATION_STREET_ADDRESS',
                       'CLASS': 'CLASS_LOCATION_TYPE', 'COMM': 'COMMODITY_RELEASED_TYPE',
                       'CSYS': 'SYSTEM_PART_INVOLVED', 'OFFSHORE': 'ON_OFF_SHORE',
                       'SHORE': 'ON_OFF_SHORE', 'OFFSHORE_TEXT': 'ON_OFF_SHORE', 'OPID': 'OPERATOR_ID',
                       'IFED': 'FEDERAL',  'INTER_INTRA': 'PIPE_FACILITY_TYPE', 'INTER_TEXT': 'PIPE_FACILITY_TYPE',
                       'INTER': 'PIPE_FACILITY_TYPE', 'TFAT': 'FATAL', 'EFAT': 'NUM_EMP_FATALITIES',
                       'FAT': 'FATAL', 'TINJ': 'INJURE', 'EINJ': 'NUM_EMP_INJURIES', 'INJ': 'INJURE',
                       'ACPRS': 'ACCIDENT_PSIG', 'INPRS': 'ACCIDENT_PSIG', 'INC_PRS': 'ACCIDENT_PSIG',
                       'MAOP': 'MOP_PSIG', 'MXPRS': 'MOP_PSIG', 'DSPRS': 'MOP_PSIG', 'PRTST': 'MOP_CFR_SECTION',
                       'TEST': 'EX_HYDROTEST_PRESSURE', 'PRTLK': 'CUSTOMER_TYPE', 'MLKD': 'MATERIAL_INVOLVED',
                       'MLKD_TEXT': 'MATERIAL_INVOLVED', 'NMDIA': 'PIPE_DIAMETER', 'NPS': 'PIPE_DIAMETER',
                       'THK': 'WT_STEEL',  'SPEC': 'PIPE_SPECIFICATION', 'PRTYR': 'INSTALLATION_YEAR',
                       'ITMYR': 'INSTALLATION_YEAR', 'MANYR': 'MANUFACTURED_YEAR', 'MANU': 'PIPE_MANUFACTURER',
                       'LOCLK': 'INCIDENT_AREA_TYPE', 'LOCLK_TEXT': 'INCIDENT_AREA_TYPE', 'PNAME': 'PREPARER_NAME',
                       'PHONE': 'PREPARER_PHONE', 'PPHONE': 'PREPARER_PHONE', 'PROT': 'UNDER_CATHODIC_PROTECTION_IND',
                       'FACAT': 'UNDER_CATHODIC_PROTECTION_IND', 'CAULK': 'CAUSE_DETAILS', 'ITYPE': 'RELEASE_TYPE',
                       'LRTYPE_TEXT': 'RELEASE_TYPE', 'ORGLK': 'ITEM_INVOLVED', 'PRTSY_TEXT': 'ITEM_INVOLVED',
                       'PRTSY': 'ITEM_INVOLVED', 'PRTFL': 'SYSTEM_PART_DETAILS', 'PRTFL_TEXT': 'SYSTEM_PART_DETAILS',
                       'LOSS': 'UNINTENTIONAL_RELEASE_BBLS', 'RECOV': 'RECOVERED_BBLS', 'FIRE': 'IGNITE_IND',
                       'IGNITE': 'IGNITE_IND', 'EXP': 'EXPLODE_IND', 'EXPLO': 'EXPLODE_IND', 'SMYS': 'PIPE_SMYS',
                       'CORRO': 'CORROSION_TYPE', 'UNINTENTIONAL_RELEASE': 'UNINTENTIONAL_RELEASE_BBLS',
                       'NAME': 'OPERATOR_NAME'}

columns_to_drop = ['DATAFILE_AS_OF', 'OPSTREET', 'OPCITY', 'OPCOUNTY', 'OPSTATE',
                   'OPZIP', 'PPPRP', 'PPPRPCURRENT', 'EMRPRP', 'ACSTREET',
                   'EMRPRPCURRENT', 'ENVPRP', 'ENVPRPCURRENT', 'OPCPRP', 'OPCPRPCURRENT',
                   'PRODPRP', 'PRODPRPCURRENT', 'OOPRP', 'OOPRPCURRENT', 'OOPPRP', 'GASPRP',
                   'GASPRPCURRENT', 'OPPRP', 'OPPRPCURRENT', 'NUM_EMP_FATALITIES',
                   'OOPPRPCURRENT', 'IPE', 'IA_IPE', 'OM_IPE', 'NUM_EMP_INJURIES',
                   'SUPPLEMENTAL_NUMBER', 'REPORT_RECEIVED_DATE', 'REPORT_TYPE',
                   'OPERATOR_STREET_ADDRESS', 'OPERATOR_CITY_NAME', 'CUSTOMER_TYPE',
                   'OPERATOR_STATE_ABBREVIATION', 'OPERATOR_POSTAL_CODE', 'IYEAR',
                   'LOCATION_POSTAL_CODE', 'ONSHORE_POSTAL_CODE',
                   'ONSHORE_CITY_NAME', 'OFF_ACCIDENT_ORIGIN', 'ONSHORE_COUNTY_NAME',
                   'OFFSHORE_COUNTY_NAME', 'ONSHORE_STATE_ABBREVIATION', 'OFFSHORE_STATE_ABBREVIATION',
                   'EST_COST_OPER_PAID', 'EST_COST_OPER_PAID_CURRENT', 'EST_COST_GAS_RELEASED',
                   'EST_COST_GAS_RELEASED_CURRENT', 'EST_COST_PROP_DAMAGE',
                   'EST_COST_PROP_DAMAGE_CURRENT', 'EST_COST_EMERGENCY',
                   'EST_COST_EMERGENCY_CURRENT', 'EST_COST_ENVIRONMENTAL', 'IDATE', 'DTHH',
                   'EST_COST_ENVIRONMENTAL_CURRENT', 'EST_COST_OTHER',
                   'EST_COST_OTHER_CURRENT', 'CORLC', 'EXT_INT_CORROSION',
                   'PREPARER_NAME', 'PREPARER_TITLE', 'PREPARER_EMAIL', 'PREPARER_PHONE',
                   'PREPARER_TELEPHONE', 'PREPARED_DATE', 'AUTHORIZER_NAME',
                   'AUTHORIZER_TITLE', 'AUTHORIZER_TELEPHONE', 'AUTHORIZER_EMAIL', 'FATALITY_IND',
                   'INJURY_IND', 'SHUTDOWN_DUE_ACCIDENT_IND', 'INCIDENT_IDENTIFIED_DATETIME',
                   'ON_SITE_DATETIME', 'DESIGNATED_NAME', 'NUM_PUB_EVACUATED',
                   'PIPE_FAC_NAME', 'SEGMENT_NAME', 'FEDERAL', 'LOCATION_TYPE',
                   'CROSSING', 'SYSTEM_PART_INVOLVED', 'DESIGNATED_LOCATION', 'WILDLIFE_IMPACT_IND',
                   'SOIL_CONTAMINATION', 'LONG_TERM_ASSESSMENT', 'REMEDIATION_IND',
                   'WATER_CONTAM_IND', 'COULD_BE_HCA', 'COMMODITY_REACHED_HCA', 'ACCIDENT_PRESSURE',
                   'PRESSURE_RESTRICTION_IND', 'PART_C_QUESTION_2_IND', 'PIPELINE_FUNCTION', 'SCADA_IN_PLACE_IND',
                   'CPM_IN_PLACE_IND', 'ACCIDENT_IDENTIFIER', 'INVESTIGATION_STATUS', 'EMPLOYEE_DRUG_TEST_IND',
                   'CONTRACTOR_DRUG_TEST_IND', 'SPILL_TYPE_CATEGORY', 'MOP_CFR_SECTION',
                   'SCADA_OPERATING_IND', 'SCADA_FUNCTIONAL_IND', 'SCADA_DETECTION_IND',
                   'SCADA_CONF_IND', 'NRC_RPT_NUM', 'NRC_RPT_DATETIME', 'INTENTIONAL_RELEASE']

order_of_columns = ['LOCAL_DATETIME', 'REPORT_NUMBER', 'SIGNIFICANT', 'SERIOUS',
                    'ON_OFF_SHORE', 'LOCATION_CITY_NAME', 'LOCATION_COUNTY_NAME',
                    'LOCATION_STATE_ABBREVIATION', 'INCIDENT_AREA_TYPE', 'CAUSE', 'MAP_CAUSE',
                    'MAP_SUBCAUSE', 'FATAL', 'INJURE', 'UNINTENTIONAL_RELEASE_BBLS', 'TOTAL_COST_IN84',
                    'TOTAL_COST_CURRENT', 'ACCIDENT_PSIG', 'MOP_PSIG', 'OPERATOR_NAME', 'OPERATOR_ID',
                    'MATERIAL_INVOLVED', 'COMMODITY_RELEASED_TYPE']


def on_offshore(df_list, index):

    """ Fix locations of datesets hl2010toPresent and gtggungs2010toPresent """

    print('Classifying onshore and offshore incidents...')

    for i in index:

        df_list[i]['LOCATION_CITY_NAME'] = df_list[i].apply(lambda x: x.ONSHORE_CITY_NAME + x.OFF_ACCIDENT_ORIGIN,
                                                            axis=1)
        df_list[i]['LOCATION_COUNTY_NAME'] = df_list[i].apply(lambda x: x.ONSHORE_COUNTY_NAME + x.OFFSHORE_COUNTY_NAME,
                                                              axis=1)
        df_list[i]['LOCATION_STATE_ABBREVIATION'] = df_list[i].apply(lambda x: x.ONSHORE_STATE_ABBREVIATION
                                                                               + x.OFFSHORE_STATE_ABBREVIATION, axis=1)


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

    return df_list


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


def rename_columns(df_list):

    """Rename dataframe columns"""

    for df in df_list:

        df.rename(columns=columns_to_rename, inplace=True)

    return df_list


def drop_columns(df_list):

    for df in df_list:

        df.drop(columns=[columns_to_drop], errors='ignore', inplace=True)

    return df_list


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

    # hl_1986_to_2001

    df_list[0].drop(columns=['COOR', 'SPLOC', 'TELRN', 'ORGLO',
                             'CAUSO', 'NFAT', 'NINJ', 'CORR', 'PREVT',
                             'JNT', 'MOP_PSIG', 'DUR', 'CAULO', 'TMPMK',
                             'FACTD', 'ONECL', 'ONEOT', 'EXCAL'], errors='ignore', inplace=True)

    df_list[0]['PIPE_FACILITY_TYPE'].replace(['YES', 'NO'], ['INTERSTATE', 'INTRASTATE'], inplace=True)

    df_list[0]['ON_OFF_SHORE'].replace(['YES', 'NO'], ['OFFSHORE', 'ONSHORE'], inplace=True)

    # gtgg_1986_to_2001

    df_list[1].drop(columns=['MPOST', 'SURVY', 'OFFAREA', 'BNUMB',
                             'OFFST', 'OCS', 'OPJUD', 'STHH',
                             'STMN', 'TELRN', 'TELRT', 'MPEST', 'PRTFO',
                             'PRTSY', 'PRTSO', 'SEAM', 'LOCLO', 'DESCO',
                             'CAUCO', 'DMGO', 'NOTIF', 'MARK', 'MRKTP',
                             'CAULO', 'STAT', 'CTEST', 'MEDO', 'MLKDO'], errors='ignore', inplace=True)

    df_list[1]['ON_OFF_SHORE'].replace(['YES', 'NO'], ['OFFSHORE', 'ONSHORE'], inplace=True)

    df_list[1]['COMMODITY_RELEASED_TYPE'] = 'NATURAL GAS'

    # gd_1986_to_2004

    df_list[9].drop(columns=['OPJUD', 'STHH', 'STMN', 'TELRN', 'TELRT',
                             'MPEST', 'NOTIF', 'MARK', 'STAT'], errors='ignore', inplace=True)

    # hl_2002_to_2009

    df_list[3].drop(columns=['DOR', 'IYEAR', 'SPILLED', 'CLASS_TEXT', 'SPUNIT_TEXT',
                             'PEMAIL', 'NFAT', 'GPFAT', 'NINJ', 'GPINJ',
                             'IO_DRUG', 'IO_ALCO'], errors='ignore', inplace=True)

    # gtgg_2002_to_2009

    df_list[6].drop(columns=['DOR', 'IYEAR', 'OCS', 'HIGHCON',
                             'PEMAIL', 'NFAT', 'GPFAT', 'NINJ', 'GPINJ', 'EVAC',
                             'EVACNO', 'STHH', 'TELRN', 'TELDT',
                             'MAOPSEC1', 'MAOPSEC2', 'MAOPSEC3', 'MAOPSEC4',
                             'MAOPSECC', 'OVERPRS', 'PLAS_DUCT', 'PLAS_BRIT',
                             'PLAS_JNT', 'TYSYS_TEXT'
                             ], errors='ignore', inplace=True)

    # gd_2004_to_2009

    df_list[8].drop(columns=['FF', 'DOR', 'IYEAR', 'OCS', 'HIGHCON',
                             'PEMAIL', 'NFAT', 'GPFAT', 'NINJ', 'GPINJ', 'EVAC',
                             'EVACNO', 'STHH', 'STMN', 'TELRN', 'TELDT',
                             'MAOPEST', 'OVERPRS', 'PLAS_DUCT', 'PLAS_BRIT',
                             'PLAS_JNT', 'TYSYS_TEXT', 'NOTIF', 'MARKED', 'PERM_MARK',
                             'MKD_IN_TIME', 'PIPE_DAMAGE', 'PRS_TEST'
                             ], errors='ignore', inplace=True)

    # hl_2010_to_Present -> nothing to clean

    # gtgg_2010_to_Present

    df_list[7].drop(columns=['INCIDENT_AREA_SUBTYPE', 'CLASS_LOCATION_TYPE', 'PIR_RADIUS',
                             'HEAT_DAMAGE_IND', 'NON_HEAT_DAMAGE_IND',
                             'HCA_FATALITIES_IND', 'EST_COST_INTENT_REL',
                             'EST_COST_INTENT_REL_CURRENT', ], errors='ignore', inplace=True)

    # gd_2010_to_Present

    df_list[5].drop(columns=['FF', 'CLASS_LOCATION_TYPE', 'INCIDENT_AREA_SUBTYPE', 'EST_COST_UNINTENTIONAL_RELEASE',
                             'EST_COST_UNINTENT_REL_CURRENT', 'EST_COST_INTENT_REL_CURRENT', 'COMMERCIAL_AFFECTED',
                             'INDUSTRIAL_AFFECTED', 'RESIDENCES_AFFECTED'], errors='ignore', inplace=True)

    # LNG_2010_to_Present

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

    return pd.concat(df_list, ignore_index=True)


def final_df(df, nan_values):

    """ Removing useless columns for visualization"""

    total_nan = df.isna().sum()

    filter_nan = total_nan > nan_values

    useless_columns = df.columns[filter_nan].to_list()

    return df.drop(columns=useless_columns)


def scrapping(url):

    """Scrap tables from url"""

    return pd.read_html(url)


def replace_state(code, state_list, code_list):

    """Replace a state code by a state name from a list"""

    for i in range(len(code_list)):

        if code == code_list[i]:

            return state_list[i]

        elif code == 'NO DATA':

            return 'NO DATA'


def upper(column):

    return column.upper()


def processing(df_list):

    """Clean data from all datasets in the directory. Filling nan values,
    renaming columns and removing useless columns"""

    print('Initiating data processing...')

    list_of_renamed_nan_df = []

    rename_columns(df_list)

    drop_columns(df_list)

    for df in df_list:

        nan_removal(df, 20)

        list_of_renamed_nan_df.append(df_clean(df, fillna_cat_col(df, nan_col_selection(df)),
                                                fillna_num_col(df, nan_col_selection(df))))

    print('Data processing successfully done')

    return concatenate_df(cleaning_individual_df(list_of_renamed_nan_df))


def df_for_visualization(df):

    return final_df(df, 6500)


def visualization_df(df, export_path):

    """Return a clean dataframe ready to visualization stage"""

    print('Cleaning final dataframe...')

    processed_df = df_clean(df, fillna_cat_col(df, nan_col_selection(df)), fillna_num_col(df, nan_col_selection(df)))

    print('Ordering columns to visualize...')

    processed_df = processed_df[order_of_columns]

    processed_df = processed_df.sort_values('LOCAL_DATETIME', ignore_index=True)

    processed_df[['TOTAL_COST_IN84', 'TOTAL_COST_CURRENT']] = \
        processed_df[['TOTAL_COST_IN84', 'TOTAL_COST_CURRENT']].astype('int')

    processed_df.loc[processed_df['ON_OFF_SHORE'].str.contains('YES') == True, ['ON_OFF_SHORE']] = 'ONSHORE'

    processed_df.loc[processed_df['ON_OFF_SHORE'].str.contains('NO') == True, ['ON_OFF_SHORE']] = 'OFFSHORE'

    print(f'Scrapping US state names from {url}')

    state_name_tables = scrapping(url)

    state_name_tables[1].rename(columns={'Territory/Associate' : 'State/District'}, inplace = True)

    states_name = pd.concat([state_name_tables[0], state_name_tables[1]])

    processed_df['LOCATION_STATE_ABBREVIATION'] = processed_df['LOCATION_STATE_ABBREVIATION'].replace(['  ', '', 'GM'],
                                                                                                      ['NO DATA',
                                                                                                       'NO DATA', 'GU'])

    processed_df['LOCATION_STATE_ABBREVIATION'] = processed_df.apply(
        lambda x: replace_state(x['LOCATION_STATE_ABBREVIATION'], list(states_name['State/District']),
                                list(states_name['Postal Code'])), axis=1)

    processed_df.rename(columns={'LOCATION_STATE_ABBREVIATION' : 'LOCATION_STATE'}, inplace=True)

    print('Unifying values in MATERIAL_INVOLVED and INCIDENT_AREA_TYPE columns')

    processed_df['INCIDENT_AREA_TYPE'].replace(
        ['UNDER GROUND', 'UNDER PAVEMENT', 'ABOVE GROUND', 'WITHIN/UNDER BUILDING'],
        ['UNDERGROUND', 'UNDERGROUND', 'ABOVEGROUND', 'INSIDE/UNDER BUILDING'], inplace=True)

    processed_df['MATERIAL_INVOLVED'].replace(
        ['POLYETHELENE PLASTIC', 'CAST/WROUGHT IRON', 'OTHER MATERIAL', 'MATERIAL OTHER THAN CARBON STEEL', 'UNKNOWN',
         'MATERIAL OTHER THAN CARBON STEEL OR PLASTIC', 'STEEL'],
        ['POLYETHYLENE PLASTIC', 'CAST IRON', 'OTHER', 'OTHER', 'OTHER', 'OTHER', 'CARBON STEEL'], inplace=True)

    print(f'Exporting data to {export_path}')

    return processed_df.to_csv(export_path, index_label=False)


def scrap_mileage(url):

    """ With this function we scrap an additional table with pipeline mileage we will use in data visualization"""

    print(f'Scrapping pipeline mileages from {url_mileage}')

    soup = BeautifulSoup(requests.get(url_mileage).content, 'html.parser')

    return soup


def find_table(soup):

    """ Locate exact table with data"""

    table = soup.find('table', {'class': 'NCSLGray'})

    return table


def get_rows(table):

    """ Obtain and process rows from table"""

    rows = table.find_all('tr')

    rows = [row.text.strip().split("\n") for row in rows]

    for row in rows:

        list_to_remove = ["", "\r"]

        for elements in list_to_remove:

            while (elements in row):

                row.remove(elements)

    clean_list = [item.strip() for row in rows for item in row]

    del clean_list[0:1]

    return clean_list


def mileage_df(list):

    """ Create final dataframe from list of rows"""

    df = pd.DataFrame([list[i:i + 7] for i in range(0, len(list), 7)])

    df.columns = df.loc[0]

    df = df.drop(df.index[0]).set_index(['Jurisdiction'])

    df = df.iloc[:-1]

    return df


def pipeline_mileage(url):

    soup = scrap_mileage(url)

    table = find_table(soup)

    rows_list = get_rows(table)

    df = mileage_df(rows_list)

    print('Creating and exporting pipeline mileage dataframe')

    return df.to_csv('./data/processed/pipeline_mileage.csv')
