from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, Row
import requests

class TwitchDataSource(DataSource):
    @classmethod
    def name(cls):
        return "twitch_stream"
    
    def schema(self):
        return StructType([
            StructField("id", StringType()),
            StructField("name", StringType()), 
            StructField("box_art_url", StringType()),
            StructField("igdb_id", StringType())
        ])
    
    def reader(self, schema: StructType):
        return TwitchSourceReader(schema, self.options)
    
class TwitchSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options

    def get_twitch_token(self):
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
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
        
    def get_twitch_top_games(self, token):
        client_id = self.options.get("client_id")
        limit = 100
        url = f"https://api.twitch.tv/helix/games/top?first={limit}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Client-Id": client_id
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("data", [])
    
    def read(self, partition):
        token = self.get_twitch_token()
        games_data = self.get_twitch_top_games(token)

        for game in games_data:
            yield Row(
                id=str(game.get("id", "")),
                name=str(game.get("name", "")),
                box_art_url=str(game.get("box_art_url", "")),
                igdb_id=str(game.get("igdb_id", "")) if game.get("igdb_id") else ""
            )