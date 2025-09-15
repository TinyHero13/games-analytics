from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, Row
import requests
import time

class TwitchTrackerDataSource(DataSource):
    @classmethod
    def name(cls):
        return "twitch_tracker"
    
    def schema(self):
        return StructType([
            StructField("id", StringType()),
            StructField("avg_viewers", StringType()),
            StructField("avg_channels", StringType()), 
            StructField("rank", StringType()),
            StructField("hours_watched", StringType())
        ])
    
    def reader(self, schema: StructType):
        return TwitchTrackerSourceReader(schema, self.options)
    
class TwitchTrackerSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options

    def read(self, partition):
        url = 'https://twitchtracker.com/api/games/summary/'
        games_id = self.options.get("game_ids", "")
        
        if isinstance(games_id, str) and games_id:
            games_id = [gid.strip() for gid in games_id.split(",") if gid.strip()]

        for game_id in games_id:         
            response = requests.get(url + str(game_id), timeout=10)
            response.raise_for_status()
            games_data = response.json()

            yield Row(
                id=str(game_id),
                avg_viewers=str(games_data.get("avg_viewers", "")),
                avg_channels=str(games_data.get("avg_channels", "")),
                rank=str(games_data.get("rank", "")),
                hours_watched=str(games_data.get("hours_watched", ""))
            )

            time.sleep(3) 