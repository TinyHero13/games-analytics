import dlt
from games_analytics.twitch_api import TwitchDataSource
from games_analytics.twitch_tracker_api import TwitchTrackerDataSource
from games_analytics.igdb_api import IGDBDataSource
from pyspark.sql import SparkSession
from pyspark.sql.functions import  current_date

client_id = dbutils.secrets.get("twitch", "client_id")
client_secret = dbutils.secrets.get("twitch", "client_secret")

spark = SparkSession.builder.appName("TwitchDataSourceApp").getOrCreate()
spark.dataSource.register(TwitchDataSource)
spark.dataSource.register(TwitchTrackerDataSource)
spark.dataSource.register(IGDBDataSource)

@dlt.table(
    comment="Twitch top games"
)
def twitch():
    return (
        spark.read
        .format("twitch_stream")
        .option("client_id", client_id)
        .option("client_secret", client_secret)
        .load()
        .withColumn("ingestion_date",  current_date())
    )

@dlt.table(
    comment="Twitch Tracker from top games"
)
def twitch_tracker():
    twitch_df = dlt.read("twitch")
    game_ids = [row["id"] for row in twitch_df.select("id").distinct().collect()]
    game_ids_str = ",".join(game_ids)
    
    return (
        spark.read
        .format("twitch_tracker")
        .option("game_ids", game_ids_str)
        .load()
        .withColumn("ingestion_date",  current_date())
    )

@dlt.table(
    comment="IGDB game metadata from top games"
)
def igdb_games():
    twitch_df = dlt.read("twitch")
    game_ids = [
        row["igdb_id"] for row in twitch_df.select("igdb_id").distinct().collect() 
        if row["igdb_id"] is not None and str(row["igdb_id"]).strip() != ""
    ]
    
    game_ids_str = ",".join(map(str, game_ids))
    
    return (
        spark.read
        .format("igdb")
        .option("game_ids", game_ids_str)
        .option("client_id", client_id)
        .option("client_secret", client_secret)
        .load()
        .withColumn("ingestion_date",  current_date())
    )