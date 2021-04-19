import argparse
import pandas as pd
from p_acquisition.m_acquisition import df_classifier


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

    print(list_of_datasets)


if __name__ == '__main__':

    arguments = argument_parser()

    main(arguments)
