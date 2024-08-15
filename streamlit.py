import os
import psycopg2
import pandas as pd 
from PIL import Image
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine

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
DB_DRIVER       =   os.getenv("DB_DRIVER")

postgres_connection = create_engine(f"{DB_DRIVER}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

get_premier_league_standings = """
    SELECT
        position,
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
        premier_league_standings
    ORDER BY
        position;
"""

final_standings_df = pd.read_sql(get_premier_league_standings, postgres_connection)

final_standings_df.set_index('position', inplace=True)

def display_standings_in_streamlit():
    st.set_page_config(
        page_title = 'Premier League Standings 2023/2024',
        page_icon = "⚽",
        layout = 'wide'
    )

    prem_league_logo_filepath  =  "./assets/premier_league_logo.png"
    prem_league_logo_image     =  Image.open(prem_league_logo_filepath)

    col1, col2 = st.columns([4, 1])
    col2.image(prem_league_logo_image) 

    st.title('🏆⚽Premier League 2023/2024 Standings⚽🏆')

    st.sidebar.title('Instructions 📖')
    st.sidebar.write("""
        The table showcases the current Premier League standings for the 2023/24 season. Toggle the visualization options to gain deeper insights!
    """)

    display_visualization = st.sidebar.radio('Would you like to view the standings as a visualization too?', ('No', 'Yes'))
    fig_specification  = px.bar(final_standings_df, 
                            x           =   'team', 
                            y           =   'points', 
                            title       =   'Premier League Standings 2023/24', 
                            labels      =   {'points':'Points', 'team':'Team', 'goals_for': 'Goals Scored', 'goals_against': 'Goals Conceded', 'goal_difference':'Goal Difference'},
                            color       =   'team',
                            height      =   600,
                            hover_data  =   ['goals_for', 'goals_against', 'goal_difference']
    )

    if display_visualization == 'Yes':
        st.table(final_standings_df)
        st.write("")
        fig = fig_specification 
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.table(final_standings_df)


display_standings_in_streamlit()