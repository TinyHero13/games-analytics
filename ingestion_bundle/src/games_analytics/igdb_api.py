from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, Row
import requests
import time
import json

class IGDBDataSource(DataSource):
    @classmethod
    def name(cls):
        return "igdb"
    
    def schema(self):
        return StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("summary", StringType()),
            StructField("rating", StringType()),
            StructField("rating_count", StringType()),
            StructField("aggregated_rating", StringType()),
            StructField("aggregated_rating_count", StringType()),
            StructField("total_rating", StringType()),
            StructField("total_rating_count", StringType()),
            StructField("first_release_date", StringType()),
            StructField("genres", StringType()),
            StructField("platforms", StringType()),
            StructField("themes", StringType()),
            StructField("game_modes", StringType()),
            StructField("player_perspectives", StringType()),
            StructField("involved_companies", StringType()),
            StructField("cover_url", StringType()),
            StructField("screenshots", StringType()),
            StructField("videos", StringType()),
            StructField("websites", StringType()),
            StructField("language_supports", StringType()),
            StructField("multiplayer_modes", StringType()),
            StructField("release_dates", StringType()),
            StructField("keywords", StringType()),
            StructField("similar_games", StringType()),
            StructField("age_ratings", StringType())
        ])
    
    def reader(self, schema: StructType):
        return IGDBSourceReader(schema, self.options)
    
class IGDBSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options

    def get_twitch_token(self):
        """Get Twitch OAuth token for IGDB API access"""
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required options")
        
        url = "https://id.twitch.tv/oauth2/token"
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()
        return response.json().get("access_token")

    def get_game_info_from_igdb(self, game_ids, token, client_id):
        """Get game information from IGDB API"""
        url = "https://api.igdb.com/v4/games"
        headers = {
            "Authorization": f"Bearer {token}",
            "Client-ID": client_id
        }
        
        if isinstance(game_ids, list):
            ids_str = ",".join(map(str, game_ids))
        else:
            ids_str = str(game_ids)

        
        data = f'''
        fields 
            name,
            summary,
            storyline,
            rating,
            rating_count,
            aggregated_rating,
            aggregated_rating_count,
            total_rating,
            total_rating_count,
            first_release_date,
            category,
            status,
            genres.name,
            platforms.name,
            platforms.abbreviation,
            themes.name,
            game_modes.name,
            player_perspectives.name,
            franchises.name,
            collections.name,
            involved_companies.company.name,
            involved_companies.developer,
            involved_companies.publisher,
            cover.url,
            artworks.url,
            screenshots.url,
            videos.video_id,
            websites.category,
            websites.url,
            age_ratings.category,
            age_ratings.rating,
            similar_games.name,
            dlcs.name,
            expansions.name,
            standalone_expansions.name,
            remakes.name,
            remasters.name,
            expanded_games.name,
            ports.name,
            forks.name,
            tags,
            keywords.name,
            language_supports.language.name,
            language_supports.language_support_type,
            multiplayer_modes.campaigncoop,
            multiplayer_modes.dropin,
            multiplayer_modes.lancoop,
            multiplayer_modes.offlinecoop,
            multiplayer_modes.offlinecoopmax,
            multiplayer_modes.offlinemax,
            multiplayer_modes.onlinecoop,
            multiplayer_modes.onlinecoopmax,
            multiplayer_modes.onlinemax,
            multiplayer_modes.platform,
            multiplayer_modes.splitscreen,
            multiplayer_modes.splitscreenonline,
            release_dates.date,
            release_dates.human,
            release_dates.platform.name,
            release_dates.region;
        where id = ({ids_str});
        limit 100;
        '''
                
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()

    def read(self, partition):
        """Read data and yield rows matching the schema"""
        try:
            token = self.get_twitch_token()
            client_id = self.options.get("client_id")
            
            games_id = self.options.get("game_ids", "")
            
            if not games_id:
                return
                        
            games_data = self.get_game_info_from_igdb(games_id, token, client_id)
            
            for game_info in games_data:
                try:
                    row = Row(
                        id=game_info.get('id'),
                        name=game_info.get('name'),
                        summary=game_info.get('summary'),
                        rating=game_info.get('rating'),
                        rating_count=game_info.get('rating_count'),
                        aggregated_rating=game_info.get('aggregated_rating'),
                        aggregated_rating_count=game_info.get('aggregated_rating_count'),
                        total_rating=game_info.get('total_rating'),
                        total_rating_count=game_info.get('total_rating_count'),
                        first_release_date=game_info.get('first_release_date'),
                        genres=json.dumps(game_info.get('genres', [])),
                        platforms=json.dumps(game_info.get('platforms', [])),
                        themes=json.dumps(game_info.get('themes', [])),
                        game_modes=json.dumps(game_info.get('game_modes', [])),
                        player_perspectives=json.dumps(game_info.get('player_perspectives', [])),
                        involved_companies=json.dumps(game_info.get('involved_companies', [])),
                        cover_url=game_info.get('cover', {}).get('url'),
                        screenshots=json.dumps([s.get('url') for s in game_info.get('screenshots', [])]),
                        videos=json.dumps([v.get('video_id') for v in game_info.get('videos', [])]),
                        websites=json.dumps(game_info.get('websites', [])),
                        language_supports=json.dumps(game_info.get('language_supports', [])),
                        multiplayer_modes=json.dumps(game_info.get('multiplayer_modes', [])),
                        release_dates=json.dumps(game_info.get('release_dates', [])),
                        keywords=json.dumps(game_info.get('keywords', [])),
                        similar_games=json.dumps(game_info.get('similar_games', [])),
                        age_ratings=json.dumps(game_info.get('age_ratings', []))
                    )
                    
                    yield row
                    
                except Exception as e:
                    continue
            
            time.sleep(1)
                    
        except Exception as e:
            raise ValueError(f"IGDB API data fetch failed: {str(e)}") from e