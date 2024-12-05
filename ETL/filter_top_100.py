import json
import boto3

TOURS = ['game-changers', 'vct-challengers', 'vct-international']

TOP_100 = {tour: {} for tour in TOURS}

s3_client = boto3.client('s3')

FILTER_STATS = {
    'avg_combat_score_per_round': 30,
    'attack_kda': 30,
    'defense_kda': 30,
    'avg_damage_dealt_per_round': 10
}

# Function to sort by a field and add unique players to the final dict
def filter_top_players(tour):
    
    # Open and load the JSON file
    with open(f'../Valorant Metadata/{tour}.json', 'r') as file:
        player_list = json.load(file)
    
    # Remove players with blank career stats
    player_list = [player for player in player_list if len(player['career_statistics']) > 0]
    
    for stat in FILTER_STATS:
        # Sort the players by the given field in descending order
        sorted_players = sorted(player_list, key=lambda x: x['career_statistics'][stat], reverse=True)
        
        count = 0
        for player in sorted_players:
            # Check if the player is already added (use 'id' to track uniqueness)
            if player['handle'] not in TOP_100[tour]:
                TOP_100[tour][player['handle']] = player  # Add player to final dict
                count += 1
            
            # Stop once we've added the required number of players
            if count == FILTER_STATS[stat]:
                break

    # put modified file into a new file
    with open(f'../Valorant Metadata/{tour}-100.json', 'w') as file:
        json.dump(list(TOP_100[tour].values()), file, indent=4)
    
    return

def chunk_players_into_files(tour):
    # Retrieve the json file with players
    with open(f'../Valorant Metadata/{tour}-100.json', 'r') as file:
        PLAYERS = json.load(file)
        
    for player in PLAYERS:
        # Upload this player's stats into its own json file
        s3_client.put_object(Bucket='esports-digital-assistant-data', Key=f"final-players-chunk/{tour}/{player['handle']}.json", Body=json.dumps(player))
        
    return

def main():
    # Iterate over all the VCT tournaments
    for tour in TOURS:
        filter_top_players(tour)
        chunk_players_into_files(tour)
    
    return
        
if __name__ == '__main__':
    main()
