import gzip
import json
from io import BytesIO
import boto3
import botocore
from botocore.errorfactory import ClientError
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime
from botocore.config import Config

# Create a custom config with increased timeout settings
s3_config = Config(
    retries={
        'max_attempts': 10,  # Increase retries in case of failures
        'mode': 'standard'
    },
    connect_timeout=300,  # Adjust as needed
    read_timeout=300      # Adjust as needed
)

# Initializing the S3 client
s3_client = boto3.client('s3', config=s3_config)
# Create a paginator for listing objects
paginator = s3_client.get_paginator('list_objects_v2')

# the source S3 bucket with the zipped data to extract from
SOURCE_S3_BUCKET = 'vcthackathon-data'
# the destination bucket to store the unzipped and transformed data
DESTINATION_S3_BUCKET = 'esports-digital-assistant-data-2'

# the 3 tours/tiers within VCT and their respective active years
TOURS = {
    # 'game-changers': [2022, 2023, 2024],
    'vct-challengers': [2023, 2024],
    # 'vct-international': [2022, 2023, 2024]
}

# the order to traverse the files in esports-data
ESPORTS_DATA = ['leagues', 'tournaments', 'teams', 'players', 'mapping_data']

# agent code mappings
with open('../Valorant Metadata/agent_code_mapping.json', 'r') as file:
    AGENT_CODE_MAPPINGS = json.load(file)

# Function definition to return the unzipped data
def extract_zipped_data(bucket, key):
    try:
        # Download the gzip file from the source S3 bucket
        gzip_obj = s3_client.get_object(Bucket=bucket, Key=key)
        gzip_content = gzip_obj['Body'].read()

        # Unzip the gzipped content
        with gzip.GzipFile(fileobj=BytesIO(gzip_content)) as gzip_file:
            return gzip_file.read()

    except NoCredentialsError:
        print("Error: Credentials are not available")
    except PartialCredentialsError:
        print("Error: Incomplete credentials provided")
    except Exception as e:
        print(f"Error: {e}")
    return

# Function definition to calculate average statistics for this player/agent
# Average statistics include attack/defense KDA, average kills/assists/combat score/revives/damage/first bloods/first deaths per round
def calculate_avg_statistics(player):
    # calculate attack KDA (attack kills + attack assists / attack deaths)
    player['attack_kda'] = round((player['total_attack_kills'] + player['total_attack_assists']) / max(1, player['total_attack_deaths']), 2)
    # calculate defense KDA (defense kills + defense assists / defense deaths)
    player['defense_kda'] = round((player['total_defense_kills'] + player['total_defense_assists']) / max(1, player['total_defense_deaths']), 2)

    # calculate average kills per round
    player['avg_kills_per_round'] = round((player['total_attack_kills'] + player['total_defense_kills']) / max(1, player['total_rounds_played']), 2)
    player['avg_assists_per_round'] = round((player['total_attack_assists'] + player['total_defense_assists']) / max(1, player['total_rounds_played']), 2)

    # delete the total kills, assists, and deaths
    for i in ['kills', 'assists', 'deaths']:
        del player[f'total_attack_{i}']
        del player[f'total_defense_{i}']

    # List of statistics other than the attack/defense KDA, average kills and assists
    stats = ['combat_score', 'revives', 'damage_dealt', 'first_bloods', 'first_deaths']
    for stat in stats:
        # Calculating and adding the average statistic to this player's summary
        player[f'avg_{stat}_per_round'] = round(player[f'total_{stat}'] / max(1, player['total_rounds_played']), 2)
        # Deleting the total statistic from this player's summary
        del player[f'total_{stat}']

    return player

# Function definition to extract zipped fandom data from the source S3 bucket,
# and load the unzipped data into our destination S3 bucket
def fandom_data_etl():
    # Traverse the fandom file path in the source S3 bucket
    for page in paginator.paginate(Bucket=SOURCE_S3_BUCKET, Prefix='fandom/'):
        # Check if the page contains objects
        if 'Contents' in page:
            for object in page['Contents']:
                key = object['Key']

                # extract, unzip, and load the unzipped data into the destination S3 bucket
                s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=key[:-3], Body=extract_zipped_data(bucket=SOURCE_S3_BUCKET, key=key))
                print(f"Uploaded unzipped file to {DESTINATION_S3_BUCKET}/{key[:-3]}")
    return

# Function definition to extract zipped tour data from the source S3 bucket,
# transform the data, and load player statistics into the destination S3 bucket
def tour_data_etl(tour):

    # Hashmap of all the leagues in this tour
    LEAGUES = {}
    # Hashmap of all the tournaments in this tour
    TOURNAMENTS = {}
    # Hashmap of all the teams participating in this tour
    TEAMS = {}
    # Hashmap of all the players participating in this tour
    PLAYERS = {}
    # Hashmap of all the games played in this tour
    GAMES = {}

    # Function definition to load esports data from this tour into one of the above hashmaps
    def esports_data_etl():
        # Traverse the esports-data by leagues, tournaments, teams, players, and finally games
        for file in ESPORTS_DATA:
            # EXRTACTION PHASE
            JSON_FILE = json.loads(extract_zipped_data(SOURCE_S3_BUCKET, f'{tour}/esports-data/{file}.json.gz'))

            # TRANSFORMATION PHASE
            # match file:
            if file == 'leagues':
                for league in JSON_FILE:
                    LEAGUES[league['league_id']] = {
                        'name': league['name'],
                        'region': league['region']
                    }
            elif file == 'tournaments':
                for tournament in JSON_FILE:
                    TOURNAMENTS[tournament['id']] = {
                        'name': tournament['name'],
                        'league_name': LEAGUES[tournament['league_id']]['name'],
                        # NOTE: we may or may not need this
                        'region': LEAGUES[tournament['league_id']]['region']
                    }
            elif file == 'teams':
                for team in JSON_FILE:
                    TEAMS[team['id']] = {
                        'name': team['name'],
                        'acronym': team['acronym'],
                        'home_league_name': LEAGUES[team['home_league_id']]['name'],
                        'region': LEAGUES[team['home_league_id']]['region']
                    }
            elif file== 'players':
                # TODO: figure out logic to map the players to the right team for that year
                # owing to increasing complexity of keeping track of the changing teams for each player,
                # we decided to only retain the most recent information of the player
                for player in JSON_FILE:
                    # get the 'created_at' for this player entry
                    date = datetime.strptime(player['updated_at'], "%Y-%m-%dT%H:%M:%SZ")
                    if (player['id'] in PLAYERS and date > PLAYERS[player['id']]['date']) or player['id'] not in PLAYERS:
                            PLAYERS[player['id']] = {
                                'handle': player['handle'],
                                'date': date,
                                'status': player['status'],
                                'first_name': player['first_name'],
                                'last_name': player['last_name'],
                                'home_team_name': TEAMS[player['home_team_id']]['name'] if player['home_team_id'] in TEAMS else None,
                                'home_team_acronym': TEAMS[player['home_team_id']]['acronym'] if player['home_team_id'] in TEAMS else None,
                                'home_league_name': TEAMS[player['home_team_id']]['home_league_name'] if player['home_team_id'] in TEAMS else None,
                                'region': TEAMS[player['home_team_id']]['region'] if player['home_team_id'] in TEAMS else None,
                                'tournament': tour,
                                # player's overall performance across all games
                                'career_statistics': {},
                                # categorize game statistics per agent/role
                                'player_statistics_per_agent': {}
                            }
            elif file== 'mapping_data':
                for game in JSON_FILE:
                    GAMES[game['platformGameId']] = {
                        'tournament': TOURNAMENTS[game['tournamentId']]['name'],
                        'region': TOURNAMENTS[game['tournamentId']]['region'],
                        # NOTE: We may or may not need the teams field since we can map each player to their respective teams with the PLAYERS dict
                        'teams': {
                            int(localTeamID): teamID for localTeamID, teamID in game['teamMapping'].items()
                        },
                        'players': {
                            int(localPlayerID): playerID for localPlayerID, playerID in game['participantMapping'].items()
                        }
                    }

        for pID, pInfo in PLAYERS.items():
            # iterate through key-value pairs in pInfo
            for key, value in pInfo.items():
                PLAYERS[pID][key] = value.isoformat() if isinstance(value, datetime) else value

        # LOADING PHASE: Optionally LOAD players metadata into the destination S3 bucket
        # if load:
        #     # Upload the PLAYERS dictionary to our destination S3 bucket
        #     s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=f'{tour}/player_metadata.json', Body=json.dumps(PLAYERS))
        #     print(f"Uploaded {tour}' players' metadata information to s3://{DESTINATION_S3_BUCKET}/{tour}/player_metadata.json")

        return

    # Function definition to load and transform player specific data
    def game_data_etl():
        # list of all trackable metrics                            
        STATS = ['total_rounds_played', 'total_attack_kills', 'total_defense_kills', 'total_attack_assists', 'total_defense_assists', 'total_attack_deaths', 'total_defense_deaths', 'total_revives', 'total_damage_dealt', 'total_combat_score', 'total_first_bloods', 'total_first_deaths']

        # parse though each game within GAMES
        for game, game_metadata in GAMES.items():
            # check for a hit for this game within {tour}/games/[2022, 2023, 2024]
            for year in TOURS[tour]:
                try:
                    # EXTRACTION PHASE
                    # check for this file within this year's directory
                    s3_client.head_object(Bucket=SOURCE_S3_BUCKET, Key=f'{tour}/games/{year}/{game}.json.gz')

                    # upon a hit, we extract, and perform transformation on the unzipped data
                    gameJSON = json.loads(extract_zipped_data(SOURCE_S3_BUCKET, f'{tour}/games/{year}/{game}.json.gz'))

                    # TRANSFORMATION PHASE
                    # Assuming players 6 - 10 are always assigned the lower team number and start as attacker (vice versa for players 1 - 5)
                    team_player_mappings = {
                        min(game_metadata['teams'].keys()): {6, 7, 8, 9, 10},
                        max(game_metadata['teams'].keys()): {1, 2, 3, 4, 5}
                    }

                    # The team that attacks first (Red) is always the team with minimum value
                    attacking_team = min(game_metadata['teams'].keys())

                    # Hashmap containing the stats per player for this game
                    game_summary = {
                        localPlayerID: {
                            # NOTE: Do we need attack/defense stats for kills, deaths, and assists?
                            'total_attack_kills': 0,
                            'total_defense_kills': 0,
                            'total_attack_assists': 0,
                            'total_defense_assists': 0,
                            'total_attack_deaths': 0,
                            'total_defense_deaths': 0,
                            'total_revives': 0,
                            'total_damage_dealt': 0,
                            'total_combat_score': 0,
                            'total_first_bloods': 0,
                            'total_first_deaths': 0,
                            'agent': None,
                            'role': None,
                        } for localPlayerID in range(1, 11)
                    }

                    config_handled = False
                    first_blood = False
                    total_rounds = 0
                    # run game analytics for this game adhering with the local player and team IDs
                    for event in gameJSON:
                        if 'configuration' in event and not config_handled:
                            # Ensure that the 'configuration' event is only handled once
                            config_handled = True

                            # assign agent information per player to their respective summaries
                            for player in event['configuration']['players']:
                                if player['selectedAgent']['fallback']['guid'] in AGENT_CODE_MAPPINGS:
                                    # Associating player with agent name
                                    game_summary[player['playerId']['value']]['agent'] = AGENT_CODE_MAPPINGS[player['selectedAgent']['fallback']['guid']]['name']
                                    # Associating player with agent role
                                    game_summary[player['playerId']['value']]['role'] = AGENT_CODE_MAPPINGS[player['selectedAgent']['fallback']['guid']]['role']
                        elif 'roundStarted' in event:
                            # if attacking team is min team number we are talking about 6 - 10
                            # else attacking team is max team number we are talking about players 1 - 5
                            attacking_team = event['roundStarted']['spikeMode']['attackingTeam']['value']
                            first_blood = False
                            total_rounds += 1
                        elif 'damageEvent' in event:
                            if 'causerId' in event['damageEvent']:
                                game_summary[event['damageEvent']['causerId']['value']]['total_damage_dealt'] += event['damageEvent']['damageAmount']
                        elif 'playerDied' in event:
                            deceasedId = event['playerDied']['deceasedId']['value']
                            killerId = event['playerDied']['killerId']['value']

                            # check if this was first blood
                            if not first_blood:
                                first_blood = True
                                game_summary[killerId]['total_first_bloods'] += 1
                                game_summary[deceasedId]['total_first_deaths'] += 1
                                pass

                            game_summary[deceasedId][f"total_{'attack' if deceasedId in team_player_mappings[attacking_team] else 'defense'}_deaths"] += 1
                            game_summary[killerId][f"total_{'attack' if killerId in team_player_mappings[attacking_team] else 'defense'}_kills"] += 1

                            # calculate assists
                            for assistant in event['playerDied']['assistants']:
                                game_summary[assistant['assistantId']['value']][f"total_{'attack' if killerId in team_player_mappings[attacking_team] else 'defense'}_assists"] += 1

                        elif 'playerRevived' in event:
                            game_summary[event['playerRevived']['revivedById']['value']]['total_revives'] += 1
                        elif 'snapshot' in event:
                            for player in event['snapshot']['players']:
                                game_summary[player['playerId']['value']]['total_combat_score'] = player['scores']['combatScore']['totalScore']

                    # Joining each player's statistics from this game to their respective entries in PLAYERS
                    for localPlayerID, playerID in game_metadata['players'].items():
                        if playerID in PLAYERS:
                            current_player = game_summary[localPlayerID]
                            current_player['total_rounds_played'] = total_rounds

                            # check to see if this the player's first game
                            if not PLAYERS[playerID]['career_statistics']:
                                PLAYERS[playerID]['career_statistics'] = {
                                    stat: 0 for stat in STATS
                                }

                            # check to see if this is the player's first time playing this role
                            if current_player['role'] not in PLAYERS[playerID]['player_statistics_per_agent']:
                                PLAYERS[playerID]['player_statistics_per_agent'][current_player['role']] = {}

                            # check to see if this is the player's first time playing this agent
                            if current_player['agent'] not in PLAYERS[playerID]['player_statistics_per_agent'][current_player['role']]:
                                PLAYERS[playerID]['player_statistics_per_agent'][current_player['role']][current_player['agent']] = {
                                    stat: 0 for stat in STATS
                                }

                            for stat in STATS:
                                PLAYERS[playerID]['career_statistics'][stat] += current_player[stat]
                                PLAYERS[playerID]['player_statistics_per_agent'][current_player['role']][current_player['agent']][stat] += current_player[stat]

                    # DEBUG statement
                    print(f'Succesfully retreived player stats from {tour}/games/{year}/{game}.json.gz')
                    
                    # need to only find the first hit
                    break
                except botocore.exceptions.ClientError as e:
                    if year == 2024:
                        print(f'Error: File for {game} not found')


        # Calculating all average statistics per player and per agent per player
        PLAYERS_LIST = []
        for _, player in PLAYERS.items():
            # Calculate overall player performance across all games
            if player['career_statistics']:
                calculate_avg_statistics(player['career_statistics'])

            # Calculate per agent statistic for this player
            if player['player_statistics_per_agent']:
                for _, role in player['player_statistics_per_agent'].items():
                    for _, agent in role.items():
                        calculate_avg_statistics(agent)

            PLAYERS_LIST.append(player)

        # LOADING PHASE
        # Upload the PLAYERS dictionary to our destination S3 bucket
        s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=f'{tour}/player_statistics.json', Body=json.dumps(PLAYERS_LIST))
        print(f"Uploaded {tour}' players' statistics information to s3://{DESTINATION_S3_BUCKET}/{tour}/player_statistics.json")

        return

    # loading leagues, tournaments, teams, and players data from this tour into the cache
    esports_data_etl()
    # creating player statistics from each game
    game_data_etl()
    return

if __name__ == "__main__":
    # extract, unzip, and load the fandom data
    # fandom_data_etl()

    # extract, unzip, and transform each tour data
    for tour in TOURS:
        tour_data_etl(tour)
