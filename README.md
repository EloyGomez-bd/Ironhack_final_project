# Exploration and machine learning modelling of US Pipeline Incidents data over the last 20 years. 

![Image](https://github.com/EloyGomez-bd/Ironhack_final_project/blob/main/data/images/quinten-de-graaf-L4gN0aeaPY4-unsplash.jpg)

This repository corresponds to Final project of Data Analytics Bootcamp at Ironhack.

The main function of the program is to retrieve a **clean dataset** obtained from US Ministry of Transportation web. This data is messy and is divided into several categories by type of handled fluid and time intervals. The final dataset with more than 18k incidents is then explored and analysed in order to look for the main insights.

Check it out at https://public.tableau.com/profile/eloy.gomez.caro.moreno#!/vizhome/ExplorationofPipelineIncidents/Dashboard1

The program also deploys a **machine learning model** based on some of the original data in order to predict the total cost of a new pipeline incident, which should be an input from the user.

## Technology Stack

* Python Programming
* Handling Pandas library
* BeautifulSoup
* Data Visualization using Tableau
* Scikit-Learn

## Installation

Use the package manager [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/) or [pip](https://pip.pypa.io/en/stable/cli/pip_install/) to install libraries in the environment you are executing the program. Required libraries are shown below:

```python
conda install pandas
conda install numpy
conda install -c conda-forge requests
conda install beautifulsoup4
conda install scipy
conda install -c conda-forge scikit-learn
pip install lightgbm
```

## Usage

Execute the file ```main.py``` from terminal or and IDE (i.e. Pycharm).

The program will process the files located in '/data/raw' and will redirect you to my profile on Tableau Public to checkout the results of exploration and analysis.

The ML model will train automatically by executing the program, but must be the user who add the new data for prediction. Such data shall be added in the file __'incidents_predict.csv'__, located in /data/results/. This may be an exmaple:


| id | FATAL | INJURE | UNINTENTIONAL_RELEASE_BBLS |	ACCIDENT_PSIG |	MOP_PSIG |
| -- | ----- | ------ | -------------------------- | -------------- | -------- |
| 0  | 1.0	 | 0.0	  | 236.0                      | 2.0	          | 100.0	   |
| 1  | 0.0	 | 0.0	  | 42.0                       | 65.0	          | 10.0	   |

|	RECOVERED_BBLS |	PIPE_DIAMETER |	PIPE_SMYS |	EX_HYDROTEST_PRESSURE |
| -------------- | -------------- | --------- | --------------------- |
| 25.0            | 6.0	          | 25000.0 	| 200.0	                |
| 7.0             | 8.0	          | 25000.0 	| 120.0	                |

| MANUFACTURED_YEAR | NORMAL_PSIG | ACCOMPANYING_LIQUID | SIGNIFICANT |	SERIOUS |	
| ----------------- | ----------- | ------------------- | ----------- | ------- |
|	2000.0	          | 12.0	      | 27.0                | YES	        | YES     |
|	1989.0	          | 78.0	      | 12.0                | YES	        | NO      |

As a result, the program will create a new file __'cost_prediction.csv'__ in the same folder.

## References

- [Pandas](https://pandas.pydata.org/pandas-docs/stable/reference/index.html)

- [Argparse](https://docs.python.org/3.7/library/argparse.html)

- [Tableau Public](https://public.tableau.com/s/)

- [Tableau viz of the day](https://public.tableau.com/es-es/gallery/?tab=viz-of-the-day&type=viz-of-the-day)

- [Scikit-Learn](https://scikit-learn.org/stable/index.html)

- [Scipy](https://www.scipy.org/)
