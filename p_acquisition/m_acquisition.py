import os
import pandas as pd

def df_classifier(path, service, sheet):

    df_list = []

    for root, dirs, files in os.walk(path):

        for filename in files:

            if filename.startswith(service):

                df_list.append(pd.read_excel(f'{path}{filename}', sheet_name=sheet))

            elif service == 'all':

                df_list.append(pd.read_excel(f'{path}{filename}', sheet_name=sheet))

                print(filename)

    return df_list
