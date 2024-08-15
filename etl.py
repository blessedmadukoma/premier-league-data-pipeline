import os
import logging
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()

API_KEY         =   os.getenv("API_KEY")
API_HOST        =   os.getenv("API_HOST")
LEAGUE_ID       =   os.getenv("LEAGUE_ID")
SEASON          =   os.getenv("SEASON")
DB_NAME         =   os.getenv("DB_NAME")
DB_USERNAME     =   os.getenv("DB_USER")
DB_PASSWORD     =   os.getenv("DB_PASS")
DB_HOST         =   os.getenv("DB_HOST")
DB_PORT         =   os.getenv("DB_PORT")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create a file handler: writes messages to a log file
file_handler = logging.FileHandler('football_table_standings.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Create a console handler: stream messages to the main console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Instantiate the logger object
logger = logging.getLogger()


# Add the file handler to the logger
logger.addHandler(file_handler)


# Add the console handler to the logger
logger.addHandler(console_handler)


url        =   "https://api-football-v1.p.rapidapi.com/v3/standings"
headers       =   {
    "X-RapidAPI-Key": API_KEY, 
                #    "X-RapidAPI-Host": API_HOST
                   }

query_string  =   {'season': SEASON,
                   'league': LEAGUE_ID}

try:
    # extract data from external API: RapidAPI
    api_response = requests.get(url, headers=headers, params=query_string, timeout=15)

    # retrieve standings
    standings_data = api_response.json()['response']

    # find the exact standings
    standings = standings_data[0]['league']['standings'][0]

    data_list = []

    for team_info in standings:
        rank = team_info['rank']
        team_name = team_info['team']['name']
        played = team_info['all']['played']
        win = team_info['all']['win']
        draw = team_info['all']['draw']
        lose = team_info['all']['lose']
        goals_for_team = team_info['all']['goals']['for']
        goals_against_team = team_info['all']['goals']['against']
        # goal_difference = goals_for_team - goals_against_team
        goal_difference = team_info['goalsDiff']
        points = team_info['points']

        data_list.append([rank, team_name, played, win, draw, lose, goals_for_team, goals_against_team, goal_difference, points])

    # Transform - convert the data into a data frame
    columns = ['P', 'Team', 'GP', 'W', 'D', 'L', 'F', 'A', 'GD', 'Pts']
    standings_df = pd.DataFrame(data_list, columns=columns)

    # display dataframe
    print(standings_df.to_string(index=False))

    # Load - store data into a postgres database

    postgres_connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = postgres_connection.cursor()

    create_table_sql_query = """
        CREATE TABLE IF NOT EXISTS premier_league_standings (
        position INTEGER PRIMARY KEY,
        team VARCHAR(255) NOT NULL,
        games_played INTEGER,
        wins INTEGER,
        draws INTEGER,
        loses INTEGER,
        goals_for INTEGER,
        goals_against INTEGER,
        goal_difference INTEGER,
        points INTEGER
        )
    """

    cur.execute(create_table_sql_query)

    postgres_connection.commit()
    
    insert_data_query = """
        INSERT INTO premier_league_standings (position, team, games_played, wins, draws, loses, goals_for, goals_against, goal_difference, points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (position) DO UPDATE SET
        team = EXCLUDED.team,
        games_played=EXCLUDED.games_played,
        wins=EXCLUDED.wins,
        draws=EXCLUDED.draws,
        loses=EXCLUDED.loses,
        goals_for=EXCLUDED.goals_for,
        goals_against=EXCLUDED.goals_against,
        goal_difference=EXCLUDED.goal_difference,
        points=EXCLUDED.points
    """

    for idx, row in standings_df.iterrows():
        cur.execute(insert_data_query, 
                    (row['P'], row['Team'], row['GP'], row['W'], row['D'], row['L'], row['F'], row['A'], row['GD'], row['Pts'])
        )

    postgres_connection.commit()

    # create an SQL view for ranked standings
    create_view_query = """
        CREATE OR REPLACE VIEW ranked_premier_league_standings AS
        SELECT
            RANK() OVER (ORDER BY points DESC, goal_difference DESC, goals_for DESC) AS position,
            team,
            games_played,
            wins,
            draws,
            loses,
            goals_for,
            goals_against,
            goal_difference,
            points
        FROM 
            premier_league_standings;
    """

    cur.execute(create_view_query)

    postgres_connection.commit()

    cur.close()
    postgres_connection.close()
except requests.HTTPError as http_err:
    logger.error(f'HTTP error occurred: {http_err}')
except requests.Timeout:
    logger.error('Request timed out after 15 seconds')
except RequestException as req_err:
    logger.error(f'Request error occurred: {req_err}')