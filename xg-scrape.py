import requests
import re
import json
import time
from pymongo import MongoClient
from scipy.stats import poisson

# URLs array
urls = [
    "https://understat.com/league/EPL/2023",
    # "https://understat.com/league/EPL/2022",
    # "https://understat.com/league/La_liga/2023"
]

# Different model ideas
# 1. Home Goals at Home vs Away Goals Away
# 2. Overall Goals
# 3. xG Home at Home vs xG Away Away
# 4. xG Overall
# Iterate the above on the following number of games:
# A) Last 4 Home / Away 
# B) Last 8 Overall
# C) This Season
# D) This Season and Last Season (If avail)

# Finally do various combinations of the above 16 possible variations

# MongoDB connection
client = MongoClient('localhost', 27017)
db = client['football_database']
team_collection = db['team_data']
match_collection = db['match_data']

# Function to aggregate records
def aggregate_records(team):
    overall_record = {
        'xG': 0, 'xGA': 0, 'scored': 0, 'missed': 0,
        'wins': 0, 'loses': 0, 'draws': 0, 'matches_played': 0
    }
    home_record = overall_record.copy()
    away_record = overall_record.copy()

    for match in team.get('history', []):
        overall_record['matches_played'] += 1
        for key in overall_record:
            if key in match and key != 'matches_played':
                overall_record[key] += match[key]

        if match['h_a'] == 'h':
            home_record['matches_played'] += 1
            for key in home_record:
                if key in match and key != 'matches_played':
                    home_record[key] += match[key]
        elif match['h_a'] == 'a':
            away_record['matches_played'] += 1
            for key in away_record:
                if key in match and key != 'matches_played':
                    away_record[key] += match[key]

    # Adding average calculations
    if overall_record['matches_played'] > 0:
        overall_record['avxG'] = overall_record['xG'] / overall_record['matches_played']
        overall_record['avxG_conceded'] = overall_record['xGA'] / overall_record['matches_played']
        overall_record['avG'] = overall_record['scored'] / overall_record['matches_played']
        overall_record['avG_conceded'] = overall_record['missed'] / overall_record['matches_played']
    if home_record['matches_played'] > 0:
        home_record['avxG'] = home_record['xG'] / home_record['matches_played']
        home_record['avxG_conceded'] = home_record['xGA'] / home_record['matches_played']
        home_record['avG'] = home_record['scored'] / home_record['matches_played']
        home_record['avG_conceded'] = home_record['missed'] / home_record['matches_played']
    if away_record['matches_played'] > 0:
        away_record['avxG'] = away_record['xG'] / away_record['matches_played']
        away_record['avxG_conceded'] = away_record['xGA'] / away_record['matches_played']
        away_record['avG'] = away_record['scored'] / away_record['matches_played']
        away_record['avG_conceded'] = away_record['missed'] / away_record['matches_played']

    return overall_record, home_record, away_record

# Function to extract league name from URL
def extract_league_name(url):
    # Extracting league name using regular expression
    match = re.search(r'/league/([^/]+)/', url)
    if match:
        return match.group(1)
    return "Unknown"

# Function to process each URL
def process_url(url):
    league_name = extract_league_name(url)
    response = requests.get(url)
    if response.status_code == 200:

        # Scrape MATCH data
        match_counter = 0
        matches = re.search(r'var datesData\s*=\s*JSON.parse\(\'(.*?)\'\)', response.text)
        if matches:
            json_data = matches.group(1).encode().decode('unicode_escape')
            match_data = json.loads(json_data)
            match_counter = len(match_data)

            for item in match_data:
                item['league'] = league_name
                match_collection.insert_one(item)

        print(f"{match_counter} MATCH records scraped for {league_name}")

        # Scrape TEAM data
        team_counter = 0
        matches = re.search(r'var teamsData\s*=\s*JSON.parse\(\'(.*?)\'\)', response.text)
        if matches:
            json_data = matches.group(1).encode().decode('unicode_escape')
            teams_data = json.loads(json_data)
            team_counter = len(teams_data)

            for team_id, team_info in teams_data.items():
                team_info['league'] = league_name
                existing_team = team_collection.find_one({'id': team_id, 'league': league_name})
                if existing_team:
                    existing_team['history'].extend(team_info['history'])
                    team_collection.update_one({'id': team_id, 'league': league_name}, {'$set': {'history': existing_team['history']}})
                else:
                    team_info['id'] = team_id
                    team_collection.insert_one(team_info)

        print(f"{team_counter} TEAM records scraped for {league_name}")

    else:
        print(f"Failed to retrieve data from {url}")

# Process each URL with a delay
for url in urls:
    process_url(url)
    time.sleep(10)  # 10-second delay between each request

# Update overall, home, and away records for each team
for team in team_collection.find():
    overall_record, home_record, away_record = aggregate_records(team)
    team_collection.update_one(
        {'_id': team['_id']},
        {'$set': {
            'overall_record': overall_record,
            'home_record': home_record,
            'away_record': away_record
        }}
    )
print("All team records updated with total matches played.")

# Function to calculate Poisson probability
def poisson_probability(lmbda, k):
    return poisson.pmf(k, lmbda)

def calculate_outcomes(avg_home_goals, avg_away_goals):
    max_goals = 6
    outcomes = {
        'home_win': 0,
        'away_win': 0,
        'draw': 0,
        'over_goals': {i: 0 for i in range(7)},
        'under_goals': {i: 0 for i in range(7)}
    }

    for home_goals in range(max_goals + 1):
        for away_goals in range(max_goals + 1):
            home_prob = poisson_probability(avg_home_goals, home_goals)
            away_prob = poisson_probability(avg_away_goals, away_goals)
            match_prob = home_prob * away_prob

            if home_goals > away_goals:
                outcomes['home_win'] += match_prob
            elif home_goals < away_goals:
                outcomes['away_win'] += match_prob
            else:
                outcomes['draw'] += match_prob

            total_goals = home_goals + away_goals
            for i in range(7):
                if total_goals > i:
                    outcomes['over_goals'][i] += match_prob
                else:
                    outcomes['under_goals'][i] += match_prob

    # Separating the odds calculation for simple outcomes and over/under goals
    odds = {key: 1 / outcomes[key] if outcomes[key] > 0 else None for key in ['home_win', 'away_win', 'draw']}
    for i in range(7):
        odds[f'under_{i}.5'] = 1 / outcomes['under_goals'][i] if outcomes['under_goals'][i] > 0 else None
        odds[f'over_{i}.5'] = 1 / (1 - outcomes['under_goals'][i]) if outcomes['under_goals'][i] > 0 else None
        

    return odds

# Function to get team data by ID
def get_team_data(team_id):
    team = team_collection.find_one({'id': team_id})
    if team:
        return team
    return None

# Function to calculate average goals in each league
def calculate_league_averages():
    league_averages = {}
    for team in team_collection.find():
        league = team.get('league')
        if not league:
            print(f"{league} League Error 1")
            continue  # Skip if league is not specified

        if 'home_record' not in team or 'away_record' not in team or 'overall_record' not in team:
            print(f"{league} League Error 2")
            continue  # Skip if required records are missing

        if league not in league_averages:
            league_averages[league] = {'total_goals': 0, 'total_home_goals': 0, 'total_away_goals': 0, 'total_matches': 0, 'total_home_matches': 0, 'total_away_matches': 0}

        league_averages[league]['total_goals'] += team['overall_record'].get('scored', 0)
        league_averages[league]['total_matches'] += team['overall_record'].get('matches_played', 0)
        league_averages[league]['total_home_goals'] += team['home_record'].get('scored', 0)
        league_averages[league]['total_home_matches'] += team['home_record'].get('matches_played', 0)
        league_averages[league]['total_away_goals'] += team['away_record'].get('scored', 0)
        league_averages[league]['total_away_matches'] += team['away_record'].get('matches_played', 0)

    for league, data in league_averages.items():
        total_matches = data['total_matches']
        total_home_matches = data['total_home_matches']
        total_away_matches = data['total_away_matches']
        data['avg_goals'] = data['total_goals'] / total_matches if total_matches > 0 else 0
        data['avg_home_goals'] = data['total_home_goals'] / total_home_matches if total_home_matches > 0 else 0
        data['avg_away_goals'] = data['total_away_goals'] / total_away_matches if total_away_matches > 0 else 0

    return league_averages

time.sleep(5)  # Make sure only runs 10 seconds after the last scrape

# Calculate league averages
league_averages = calculate_league_averages()

# Function to calculate expected goals and odds
def calculate_expected_goals_and_odds(home_team_data, away_team_data, league_averages, league, model_type):
    avg_goals_league = league_averages[league]['avg_goals']
    avg_home_goals_league = league_averages[league]['avg_home_goals']
    avg_away_goals_league = league_averages[league]['avg_away_goals']

    if model_type == "actual_overall":
        home_attack_strength = home_team_data['overall_record']['avG'] / avg_goals_league
        away_defence_strength = away_team_data['overall_record']['avG_conceded'] / avg_goals_league
        away_attack_strength = away_team_data['overall_record']['avG'] / avg_goals_league
        home_defence_strength = home_team_data['overall_record']['avG_conceded'] / avg_goals_league
    elif model_type == "actual_home_away":
        home_attack_strength = home_team_data['home_record']['avG'] / avg_home_goals_league
        away_defence_strength = away_team_data['away_record']['avG_conceded'] / avg_away_goals_league
        away_attack_strength = away_team_data['away_record']['avG'] / avg_away_goals_league
        home_defence_strength = home_team_data['home_record']['avG_conceded'] / avg_home_goals_league
    elif model_type == "xg_home_away":
        home_attack_strength = home_team_data['home_record']['avxG'] / avg_home_goals_league
        away_defence_strength = away_team_data['away_record']['avxG_conceded'] / avg_away_goals_league
        away_attack_strength = away_team_data['away_record']['avxG'] / avg_away_goals_league
        home_defence_strength = home_team_data['home_record']['avxG_conceded'] / avg_home_goals_league
    else:  # xg_overall or other models
        home_attack_strength = home_team_data['overall_record']['avxG'] / avg_goals_league
        away_defence_strength = away_team_data['overall_record']['avxG_conceded'] / avg_goals_league
        away_attack_strength = away_team_data['overall_record']['avxG'] / avg_goals_league
        home_defence_strength = home_team_data['overall_record']['avxG_conceded'] / avg_goals_league

    # Calculate expected goals
    xGHome = home_attack_strength * away_defence_strength * avg_home_goals_league
    xGAway = away_attack_strength * home_defence_strength * avg_away_goals_league

    # Calculate odds
    odds = calculate_outcomes(xGHome, xGAway)
    return odds, xGHome, xGAway

# Process each match to calculate odds
for match in match_collection.find():
    league = match.get('league')
    if not league or league not in league_averages:
        continue  # Skip if the league data is not available or not specified

    home_team_id = match['h']['id']
    away_team_id = match['a']['id']

    home_team_data = get_team_data(home_team_id)
    away_team_data = get_team_data(away_team_id)

    if home_team_data and away_team_data:
        # Calculate odds for each model
        odds_xG_overall, xGHome_overall, xGAway_overall = calculate_expected_goals_and_odds(home_team_data, away_team_data, league_averages, league, "xg_overall")
        odds_actual_overall, _, _ = calculate_expected_goals_and_odds(home_team_data, away_team_data, league_averages, league, "actual_overall")
        odds_actual_home_away, _, _ = calculate_expected_goals_and_odds(home_team_data, away_team_data, league_averages, league, "actual_home_away")
        odds_xg_home_away, _, _ = calculate_expected_goals_and_odds(home_team_data, away_team_data, league_averages, league, "xg_home_away")

        # Update match document with calculated odds for each model
        match_collection.update_one(
            {'_id': match['_id']},
            {'$set': {
                'odds_xG_overall': odds_xG_overall,
                'odds_actual_overall': odds_actual_overall,
                'odds_actual_home_away': odds_actual_home_away,
                'odds_xg_home_away': odds_xg_home_away,
                'league_averages': league_averages[league],
                'home': home_team_data,
                'away': away_team_data,
            }}
        )

print("Match odds for all models updated.")

exampleTeamData = {
  "_id": {
    "$oid": "657391867248d9d792fa41ed"
  },
  "id": "71",
  "title": "Aston Villa",
  "history": [
    {
      "h_a": "a",
      "xG": 1.486,
      "xGA": 4.32208,
      "npxG": 1.486,
      "npxGA": 4.32208,
      "ppda": {
        "att": 324,
        "def": 28
      },
      "ppda_allowed": {
        "att": 255,
        "def": 17
      },
      "deep": 6,
      "deep_allowed": 9,
      "scored": 1,
      "missed": 5,
      "xpts": 0.1709,
      "result": "l",
      "date": "2023-08-12 16:30:00",
      "wins": 0,
      "draws": 0,
      "loses": 1,
      "pts": 0,
      "npxGD": -2.83608
    }
  ],
  "league": "EPL",
  "away_record": {
    "xG": 64.68272400000001,
    "xGA": 94.784208,
    "scored": 56,
    "missed": 80,
    "wins": 18,
    "loses": 22,
    "draws": 14,
    "matches_played": 54,
    "avxG": 1.1978282222222223,
    "avxGA": 1.7552631111111112
  },
  "home_record": {
    "xG": 96.20749199999999,
    "xGA": 57.80858800000001,
    "scored": 114,
    "missed": 52,
    "wins": 38,
    "loses": 10,
    "draws": 4,
    "matches_played": 52,
    "avxG": 1.8501440769230766,
    "avxGA": 1.1117036153846156
  },
  "overall_record": {
    "xG": 160.89021599999998,
    "xGA": 152.59279599999994,
    "scored": 170,
    "missed": 132,
    "wins": 56,
    "loses": 32,
    "draws": 18,
    "matches_played": 106,
    "avxG": 1.517832226415094,
    "avxGA": 1.4395546792452825
  }
}