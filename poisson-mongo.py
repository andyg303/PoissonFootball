import pandas as pd
from scipy.stats import poisson
from pymongo import MongoClient

# MongoDB connection
client = MongoClient('localhost', 27017)  # Change host and port if needed
db = client['soccer_database']            # Database name
collection = db['match_odds']             # Collection name

# Load your data
data = pd.read_csv('premier-league.csv')

# Calculate league average goals scored and conceded
league_avg_goals_scored = (data['Criteria1Val'].sum() + data['Criteria2Val'].sum()) / (2 * len(data))
league_avg_goals_conceded = (data['Criteria6Val'].sum() + data['Criteria7Val'].sum()) / (2 * len(data))

# Function to calculate match probabilities using Poisson distribution
def calculate_probabilities(home_goals_scored, away_goals_scored, home_goals_conceded, away_goals_conceded):
    # Adjusted expectancy calculations
    home_goals_expectancy = home_goals_scored * away_goals_conceded / league_avg_goals_conceded
    away_goals_expectancy = away_goals_scored * home_goals_conceded / league_avg_goals_conceded

    max_goals = 10
    home_probs = [poisson.pmf(i, home_goals_expectancy) for i in range(0, max_goals+1)]
    away_probs = [poisson.pmf(i, away_goals_expectancy) for i in range(0, max_goals+1)]

    home_win_prob = sum(home_probs[i] * sum(away_probs[:i]) for i in range(1, max_goals+1))
    away_win_prob = sum(away_probs[i] * sum(home_probs[:i]) for i in range(1, max_goals+1))
    draw_prob = sum(home_probs[i] * away_probs[i] for i in range(max_goals+1))

    over_under_probs = {}
    for threshold in range(6):
        over_prob = sum(home_probs[i] * sum(away_probs[j] for j in range(threshold+1, max_goals+1)) for i in range(max_goals+1))
        over_under_probs[f"Over {threshold}.5"] = over_prob
        over_under_probs[f"Under {threshold}.5"] = 1 - over_prob

    return home_win_prob, away_win_prob, draw_prob, over_under_probs

# Convert probability to decimal odds
def prob_to_odds(prob):
    return round(1 / prob, 2) if prob > 0 else None

# Check if BF odds offer value
def is_value_bet(bf_odds, calc_odds):
    return "TRUE" if bf_odds > calc_odds else ""

# Prepare the data for CSV
output_data = []
for index, row in data.iterrows():
    home_goals_expectancy = row['Criteria1Val'] * league_avg_goals_scored
    away_goals_expectancy = row['Criteria2Val'] * league_avg_goals_scored
    home_goals_conceded = row['Criteria6Val']
    away_goals_conceded = row['Criteria7Val']

    home_win_prob, away_win_prob, draw_prob, over_under_probs = calculate_probabilities(home_goals_expectancy, away_goals_expectancy, home_goals_conceded, away_goals_conceded)

    calc_home_win_odds = prob_to_odds(home_win_prob)
    calc_draw_odds = prob_to_odds(draw_prob)
    calc_away_win_odds = prob_to_odds(away_win_prob)

    match_data = {
        'Match': f"{row['Home']} vs {row['Away']}",
        'Calculated Home Win Odds': calc_home_win_odds,
        'BFHomeOdds': row['Criteria3Val'],
        'IsHomeValue': is_value_bet(row['Criteria3Val'], calc_home_win_odds),
        'Calculated Draw Odds': calc_draw_odds,
        'BFDrawOdds': row['Criteria4Val'],
        'IsDrawValue': is_value_bet(row['Criteria4Val'], calc_draw_odds),
        'Calculated Away Win Odds': calc_away_win_odds,
        'BFAwayOdds': row['Criteria5Val'],
        'IsAwayValue': is_value_bet(row['Criteria5Val'], calc_away_win_odds)
    }
    
    for key, value in over_under_probs.items():
        match_data[key + ' Odds'] = prob_to_odds(value)

    # Add the match_data to MongoDB
    collection.insert_one(match_data)
    
print("Refactored odds predictions saved to MongoDB")
