import argparse
from p_acquisition.m_acquisition import df_classifier
from p_wrangling.m_wrangling import pre_processing, processing, visualization_df, pipeline_mileage, url_mileage,\
    df_for_visualization, export_path, analysis_path, columns_to_drop
from p_reporting.m_reporting import visualization_tableau, url


def argument_parser():
    """
    parse arguments to script
    """

    parser = argparse.ArgumentParser()

    # Arguments here
    parser.add_argument("-p", "--path", help="specify database location", type=str, required=True)

    # Arguments here

    args = parser.parse_args()

    return args


def main(arguments):

    print('Starting process...')

    path = arguments.path

    print(f'Obtaining data from "{path}"')

    list_of_datasets = df_classifier(path, 'all', 1)

    preprocessed_df = pre_processing(list_of_datasets)

    merged_stuff = processing(preprocessed_df)

    print('Creating dataframe for visualization')

    visualization_df(df_for_visualization(merged_stuff), export_path)

    print('Creating dataframe for machine learning modelling')

    merged_stuff.drop(columns=columns_to_drop, errors='ignore', inplace=True)

    merged_stuff.to_csv(analysis_path, index=False)

    pipeline_mileage(url_mileage)

    visualization_tableau(url)


if __name__ == '__main__':

    arguments = argument_parser()

    main(arguments)
