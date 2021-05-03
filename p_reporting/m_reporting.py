import webbrowser

url = 'https://public.tableau.com/profile/eloy.gomez.caro.moreno#!/vizhome/ExplorationofPipelineIncidents/Dashboard1'


def visualization_tableau(url):

    """Open Tableau Visualization Dashboard on Tableau Public"""

    return webbrowser.open(url)