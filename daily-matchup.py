# -*- coding: utf-8 -*-
"""
Created on Fri May  5 07:40:00 2023

@author: Local User
"""

import pandas as pd
import statsapi
from statsapi import player_stat_data
import requests
from datetime import datetime, timedelta
import numpy as np
import math
import sqlalchemy
import mysql.connector
import json
import meteostat

Teams_and_IDs = pd.read_csv("Teams_and_IDs.csv")
Player_Positions = pd.read_csv("MLB_Player_Positions.csv")
Park_Data = pd.read_csv("mlb_parks.csv")

All_Teams_Data = []

for team_id in Teams_and_IDs['Team_ID']:

    roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=allTime"

    response = requests.get(roster_url)

    roster_data = response.json()['roster']

    for player in roster_data:
        
        Player_Name = player['person']['fullName']
        Player_ID = player['person']['id']
        
        Player_Dictionary = {"Team_ID":team_id,"Player Name":Player_Name, "Player ID":Player_ID}
        
        All_Teams_Data.append(Player_Dictionary)

All_Teams_DataFrame = pd.DataFrame(All_Teams_Data)

def Team_To_ID(team_name):

    
    team_id = Teams_and_IDs["Team_ID"][Teams_and_IDs['Team_Name'] == team_name].iloc[0]

    return team_id

def ID_To_Team(team_id):
    
    team_name = Teams_and_IDs["Team_Name"][Teams_and_IDs['Team_ID'] == team_id].iloc[0]
    
    return team_name

def Player_to_ID(player_name):
    
    player_id = Player_Positions["player_id"][Player_Positions["player_name"] == player_name]
    
    if len(player_id) >= 1:
        
        return player_id.iloc[0]
    
    else:
        
        return ""
    
def odds_calculator(probability):

    return round(-100 / ((1/probability)-1))

def celsius_to_fahrenheit(celsius):
    
    return ( celsius * (9/5) ) + 32 

# =============================================================================
# Start
# =============================================================================

Schedule = statsapi.schedule(start_date = datetime.today().strftime("%Y-%m-%d"), end_date = datetime.today().strftime("%Y-%m-%d"))
Schedule_DataFrame = pd.json_normalize(Schedule)

# Schedule_DataFrame = Schedule_DataFrame[(Schedule_DataFrame["status"] == "Final") & (Schedule_DataFrame["game_date"] > "2022-04-01")]

game_id_list = list(Schedule_DataFrame["game_id"].drop_duplicates())

game_matchups = []

for game_id in game_id_list:
    
    start = datetime.now()
    
    home_batting_matchup, away_batting_matchup = [], []
    
    Game = Schedule_DataFrame[Schedule_DataFrame["game_id"] == game_id]
    
    home_team_name, home_team_id = Game["home_name"].iloc[0], Game["home_id"].iloc[0]
    away_team_name, away_team_id = Game["away_name"].iloc[0], Game["away_id"].iloc[0]

    home_probable_pitcher = Game["home_probable_pitcher"].iloc[0]
    away_probable_pitcher = Game["away_probable_pitcher"].iloc[0]
    
    game_boxscore = statsapi.boxscore_data(game_id)
    
    all_home_players = game_boxscore["home"]["players"]
    
    home_players_list = []
    
    for home_player in all_home_players:
        
        home_player = all_home_players[home_player]
        
        home_player_name, home_player_id = home_player["person"]["fullName"], home_player["person"]["id"]
        
        home_player_dict = {"name":home_player_name, "id":home_player_id}
        home_players_list.append(home_player_dict)
        
    home_players_dataframe = pd.DataFrame(home_players_list)
    
    home_batting_lineup_ids = game_boxscore["home"]["bench"]
    
    home_batters = home_players_dataframe[home_players_dataframe["id"].isin(home_batting_lineup_ids)]
       
    home_batter_stats_list = []
    
    for home_batter in home_batters["id"]:
        
        home_batter_stats = pd.json_normalize(statsapi.player_stat_data(personId = home_batter, group="hitting", type="yearByYear", sportId=1)["stats"], max_level = 0)
        
        valid_home_batter_season_stats = home_batter_stats[home_batter_stats["season"] == str(2022)]["stats"].drop_duplicates(keep = "last")
        
        if len(valid_home_batter_season_stats) < 1:
            
            valid_home_batter_season_stats = home_batter_stats[home_batter_stats["season"] == str(2023)]["stats"].drop_duplicates(keep = "last")           
            
        if len(valid_home_batter_season_stats) < 1:
            continue
            
        season_batter_stats = valid_home_batter_season_stats.iloc[0]
        season_batter_stats["name"] = home_batters["name"][home_batters["id"] == home_batter].iloc[0]
        season_batter_stats["id"] = home_batters["id"][home_batters["id"] == home_batter].iloc[0]
        
        home_batter_stats_list.append(season_batter_stats)
        
    home_team_batting_stats = pd.DataFrame(home_batter_stats_list).drop_duplicates(subset = "name", keep ="last")
    
    home_opposing_pitcher_name, home_opposing_pitcher_id = Game["away_probable_pitcher"].iloc[0], Player_to_ID(Game["away_probable_pitcher"].iloc[0])
    
    # If there's no data for the home pitcher, just move on to the away
    
    
    # if you don't have an ID for the pitcher
    
    if type(home_opposing_pitcher_id) != type(np.int64()):
        pass
    else:

        home_opposing_pitcher_stats = pd.json_normalize(statsapi.player_stat_data(personId = home_opposing_pitcher_id, group="pitching", type="yearByYear", sportId=1)["stats"], max_level = 0)
    
        # If there is just no data from the API for this player
        
        if len(home_opposing_pitcher_stats) == 0:
            pass
        else:
        
            valid_home_opposing_pitcher_season_stats = home_opposing_pitcher_stats[home_opposing_pitcher_stats["season"] == str(2022)]["stats"].drop_duplicates(keep = "last")
        
            if len(valid_home_opposing_pitcher_season_stats) < 1:
                
                valid_home_opposing_pitcher_season_stats = home_opposing_pitcher_stats[home_opposing_pitcher_stats["season"] == str(2023)]["stats"].drop_duplicates(keep = "last")
        
            # If there is no historical data for last or current season
        
            if len(valid_home_opposing_pitcher_season_stats) == 0:
                pass
            else:
    
                home_opposing_pitcher_season_stats = valid_home_opposing_pitcher_season_stats.iloc[0]
                home_opposing_pitcher_season_stats["name"] = home_opposing_pitcher_name
                home_opposing_pitcher_season_stats["id"] = home_opposing_pitcher_id
                
                
                home_team_opposing_pitcher_stats_dataframe = pd.DataFrame([home_opposing_pitcher_season_stats])
                
                home_team_opposing_pitcher_stats = pd.concat([home_team_opposing_pitcher_stats_dataframe]*len(home_team_batting_stats))
                
                home_batting_matchup = pd.concat([home_team_opposing_pitcher_stats.reset_index(drop = True).add_prefix("pitching_"), home_team_batting_stats.reset_index(drop = True).add_prefix("batting_")], axis = 1)
            
                # =============================================================================
                # End of calculating the side for the batters on the home team
                # =============================================================================
            
    all_away_players = game_boxscore["away"]["players"]
    
    away_players_list = []
    for away_player in all_away_players:
        
        away_player = all_away_players[away_player]
        
        away_player_name, away_player_id = away_player["person"]["fullName"], away_player["person"]["id"]
        
        away_player_dict = {"name":away_player_name, "id":away_player_id}
        away_players_list.append(away_player_dict)
        
    away_players_dataframe = pd.DataFrame(away_players_list)
    
    away_batting_lineup_ids = game_boxscore["away"]["bench"]
    
    away_batters = away_players_dataframe[away_players_dataframe["id"].isin(away_batting_lineup_ids)]
    
    away_batter_stats_list = []
    
    for away_batter in away_batters["id"]:
        
        away_batter_stats = pd.json_normalize(statsapi.player_stat_data(personId = away_batter, group="hitting", type="yearByYear", sportId=1)["stats"], max_level = 0)
        
        # if len(away_batter_stats) == 0:
        #     continue
        
        valid_away_batter_season_stats = away_batter_stats[away_batter_stats["season"] == str(2022)]["stats"].tail(1)
        
        if len(valid_away_batter_season_stats) < 1:
            valid_away_batter_season_stats = away_batter_stats[away_batter_stats["season"] == str(2023)]["stats"].tail(1)
            
        if len(valid_away_batter_season_stats) < 1:
            continue
        
        season_batter_stats = valid_away_batter_season_stats.iloc[0]
        season_batter_stats["name"] = away_batters["name"][away_batters["id"] == away_batter].iloc[0]
        season_batter_stats["id"] = away_batters["id"][away_batters["id"] == away_batter].iloc[0]
        
        away_batter_stats_list.append(season_batter_stats)
        
    away_team_batting_stats = pd.DataFrame(away_batter_stats_list).drop_duplicates(subset = "name", keep ="last")
        
    away_opposing_pitcher_name, away_opposing_pitcher_id = Game["home_probable_pitcher"].iloc[0], Player_to_ID(Game["home_probable_pitcher"].iloc[0])
    
    if type(away_opposing_pitcher_id) != type(np.int64()):
        pass
    else:
    
        away_opposing_pitcher_stats = pd.json_normalize(statsapi.player_stat_data(personId = away_opposing_pitcher_id, group="pitching", type="yearByYear", sportId=1)["stats"], max_level = 0)
        
        if len(away_opposing_pitcher_stats) == 0:
            pass
        else:
        
            valid_away_opposing_pitcher_season_stats = away_opposing_pitcher_stats[away_opposing_pitcher_stats["season"] == str(2022)]["stats"].drop_duplicates(keep = "last")
        
            if len(valid_away_opposing_pitcher_season_stats) < 1:
                
                valid_away_opposing_pitcher_season_stats = away_opposing_pitcher_stats[away_opposing_pitcher_stats["season"] == str(2023)]["stats"].drop_duplicates(keep = "last")
            
            if len(valid_away_opposing_pitcher_season_stats) == 0:
                pass
            else:
            
                away_opposing_pitcher_season_stats = valid_away_opposing_pitcher_season_stats.iloc[0]
                away_opposing_pitcher_season_stats["name"] = away_opposing_pitcher_name
                away_opposing_pitcher_season_stats["id"] = away_opposing_pitcher_id
                
                away_team_opposing_pitcher_stats_dataframe = pd.DataFrame([away_opposing_pitcher_season_stats])
                        
                # if len(away_team_opposing_pitcher_stats_dataframe) == 0:    
                #     continue
                        
                away_team_opposing_pitcher_stats = pd.concat([away_team_opposing_pitcher_stats_dataframe]*len(away_team_batting_stats))
                
                away_batting_matchup = pd.concat([away_team_opposing_pitcher_stats.reset_index(drop = True).add_prefix("pitching_"), away_team_batting_stats.reset_index(drop = True).add_prefix("batting_")], axis = 1)
            
                # =============================================================================
                # End of calculating the side for the batters on the away team
                # =============================================================================
    
    
    if (len(home_batting_matchup) == 0) and (len(away_batting_matchup) == 0):
        
        continue
    
    elif (len(home_batting_matchup) >= 1) and (len(away_batting_matchup) == 0):
        
        game_matchup = home_batting_matchup.reset_index(drop = True).copy()
        game_matchup["game_id"] = Game["game_id"].iloc[0]
        game_matchup["game_venue"] = Game["venue_name"].iloc[0]
        game_matchup["game_date"] = Game["game_date"].iloc[0]
        game_matchup["game_datetime"] = Game["game_datetime"].iloc[0]
        
        if Game["venue_name"].isin(list(Park_Data["Venue"])).iloc[0] == False:
            continue
        
        park_lat = Park_Data["latitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
        park_lon = Park_Data["longitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
    
        point_object = meteostat.Point(lat = park_lat, lon = park_lon)

        historical_weather = meteostat.Hourly(loc = point_object, start = (pd.to_datetime(game_matchup["game_date"].iloc[0])), end = (pd.to_datetime(game_matchup["game_date"].iloc[0]) + timedelta(days = 1)), timezone = "America/Chicago").fetch().reset_index()

        pre_game_weather = historical_weather[historical_weather["time"] <= (pd.to_datetime(Game["game_datetime"].iloc[0])).tz_convert("America/Chicago")]
        last_hour_weather = pre_game_weather.tail(1).copy()
        last_hour_weather["temp_f"] = last_hour_weather["temp"].apply(celsius_to_fahrenheit).iloc[0]
        
        game_temperature = last_hour_weather["temp_f"].iloc[0]
        
        game_matchup["temp"] = game_temperature

        game_matchups.append(game_matchup)
        
    elif (len(home_batting_matchup) == 0) and (len(away_batting_matchup) >= 1):
        
        game_matchup = away_batting_matchup.reset_index(drop = True).copy()
        game_matchup["game_id"] = Game["game_id"].iloc[0]
        game_matchup["game_venue"] = Game["venue_name"].iloc[0]
        game_matchup["game_date"] = Game["game_date"].iloc[0]
        game_matchup["game_datetime"] = Game["game_datetime"].iloc[0]
        
        if Game["venue_name"].isin(list(Park_Data["Venue"])).iloc[0] == False:
            continue
        
        park_lat = Park_Data["latitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
        park_lon = Park_Data["longitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
    
        point_object = meteostat.Point(lat = park_lat, lon = park_lon)

        historical_weather = meteostat.Hourly(loc = point_object, start = (pd.to_datetime(game_matchup["game_date"].iloc[0])), end = (pd.to_datetime(game_matchup["game_date"].iloc[0]) + timedelta(days = 1)), timezone = "America/Chicago").fetch().reset_index()

        pre_game_weather = historical_weather[historical_weather["time"] <= (pd.to_datetime(Game["game_datetime"].iloc[0])).tz_convert("America/Chicago")]
        last_hour_weather = pre_game_weather.tail(1).copy()
        last_hour_weather["temp_f"] = last_hour_weather["temp"].apply(celsius_to_fahrenheit).iloc[0]
        
        game_temperature = last_hour_weather["temp_f"].iloc[0]
        
        game_matchup["temp"] = game_temperature


        game_matchups.append(game_matchup)
        
    elif (len(home_batting_matchup) >= 0) and (len(away_batting_matchup) >= 1):
        
        game_matchup = pd.concat([home_batting_matchup, away_batting_matchup], axis = 0).reset_index(drop = True)
        game_matchup["game_id"] = Game["game_id"].iloc[0]
        game_matchup["game_venue"] = Game["venue_name"].iloc[0]
        game_matchup["game_date"] = Game["game_date"].iloc[0]
        game_matchup["game_datetime"] = Game["game_datetime"].iloc[0]
        
        if Game["venue_name"].isin(list(Park_Data["Venue"])).iloc[0] == False:
            continue
        
        park_lat = Park_Data["latitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
        park_lon = Park_Data["longitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
    
        point_object = meteostat.Point(lat = park_lat, lon = park_lon)

        historical_weather = meteostat.Hourly(loc = point_object, start = (pd.to_datetime(game_matchup["game_date"].iloc[0])), end = (pd.to_datetime(game_matchup["game_date"].iloc[0]) + timedelta(days = 1)), timezone = "America/Chicago").fetch().reset_index()

        pre_game_weather = historical_weather[historical_weather["time"] <= (pd.to_datetime(Game["game_datetime"].iloc[0])).tz_convert("America/Chicago")]
        last_hour_weather = pre_game_weather.tail(1).copy()
        last_hour_weather["temp_f"] = last_hour_weather["temp"].apply(celsius_to_fahrenheit).iloc[0]
        
        game_temperature = last_hour_weather["temp_f"].iloc[0]
        
        game_matchup["temp"] = game_temperature

    
        game_matchups.append(game_matchup)
    
    end = datetime.now()
    print(f"Elapsed Time: {end - start}")
    
# =============================================================================
# End    
# =============================================================================
    
game_matchup_dataframe = pd.concat(game_matchups).dropna()

save_to_file_string = f"prediction-{datetime.today().strftime('%Y-%m-%d')}.csv"
print(save_to_file_string)
print(len(game_matchup_dataframe.drop_duplicates("game_id")))
game_matchup_dataframe.to_csv(f"{save_to_file_string}")
