# Exploration and machine learning modelling of US Pipeline Incidents data over the last 20 years. 

![Image](https://github.com/EloyGomez-bd/Ironhack_final_project/blob/main/data/images/quinten-de-graaf-L4gN0aeaPY4-unsplash.jpg)

This repository corresponds to Final project of Data Analytics Bootcamp at Ironhack.

The main function of the program is to retrieve a clean dataset obtained from US Ministry of Transportation web. This data is messy and is divided into several categories by type of handled fluid and time intervals. The final dataset with more than 18k incidents is then explored and analysed in order to look for the main insights.

Check it out at https://public.tableau.com/profile/eloy.gomez.caro.moreno#!/vizhome/ExplorationofPipelineIncidents/Dashboard1

## Technology Stack

- Python Programming
- Handling Pandas library
- BeautifulSoup
- Data Visualization using Tableau
- Scikit-Learn


## Installation

Use the package manager [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/) to install libraries in the environment you are executing the program. Required libraries are shown below:

```python
conda install pandas
conda install -c conda-forge scikit-learn 
```

## Usage

Execute the file ```main.py --path './data/raw/'``` from terminal or and IDE (i.e. Pycharm).


## References

- [Pandas](https://pandas.pydata.org/pandas-docs/stable/reference/index.html)

- [Argparse](https://docs.python.org/3.7/library/argparse.html)

- [Tableau Public](https://public.tableau.com/s/)

- [Tableau viz of the day](https://public.tableau.com/es-es/gallery/?tab=viz-of-the-day&type=viz-of-the-day)

- [Scikit-Learn](https://scikit-learn.org/stable/index.html)
