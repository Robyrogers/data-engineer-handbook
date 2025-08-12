from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import Window

spark = SparkSession.builder.appName('Jupyter')\
    .config(map={
        'spark.sql.autoBroadcastJoinThreshold': -1
    })\
    .getOrCreate()

## Load matches data
matches = spark.read.options(
    header = True,
    inferSchema = True
).csv('/home/iceberg/data/matches.csv')
(
    matches
        .writeTo('bootcamp.matches')
        .tableProperty('write.bucket-by-columns', 'match_id, playlist_id')
        .tableProperty('write.bucket-by-number', '4')
        .partitionedBy(
            sf.years('completion_date'),
            'mapid'
        )
        .createOrReplace()
)

## Load match details data
match_details = spark.read.options(
    header = True,
    inferSchema = True
).csv('/home/iceberg/data/match_details.csv')
(
    match_details
        .writeTo('bootcamp.match_details')
        .tableProperty('write.bucket-by-columns', 'match_id')
        .tableProperty('write.bucket-by-number', '4')
        .createOrReplace()
)

## Load medals per matches for per players data
medals_matches_players = spark.read.options(
    header = True,
    inferSchema = True
).csv('/home/iceberg/data/medals_matches_players.csv')
(
    medals_matches_players
        .writeTo('bootcamp.medals_matches_players')
        .tableProperty('write.bucket-by-columns', 'match_id, player_gamertag, medal_id')
        .tableProperty('write.bucket-by-number', '4')
        .createOrReplace()
)

## Load medals data
medals = spark.read.options(
    header = True,
    inferSchema = True
).csv('/home/iceberg/data/medals.csv')
(
    medals
        .writeTo('bootcamp.medals')
        .tableProperty('write.bucket-by-columns', 'medal_id')
        .tableProperty('write.bucket-by-number', '4')
        .createOrReplace()
)

## Load maps data
maps = spark.read.options(
    header = True,
    inferSchema = True
).csv('/home/iceberg/data/maps.csv')
(
    maps
        .writeTo('bootcamp.maps')
        .tableProperty('write.bucket-by-columns', 'mapid')
        .tableProperty('write.bucket-by-number', '4')
        .createOrReplace()
)

## Aggregate matches, match_details and medal achievements
match_player_medals = spark.table('bootcamp.match_details').alias('md').join(
    spark.table('bootcamp.matches').alias('m'),
    'match_id'
).join(
    spark.table('bootcamp.medals_matches_players').alias('mmp'),
    ['match_id', 'player_gamertag']
).cache()

## Aggregate some common player stats per match
match_players_stats = match_player_medals.groupBy(
    'match_id',
    'player_gamertag'
).agg(
    sf.max('mapid').alias('map_id'),
    sf.max('playlist_id').alias('playlist_id'),
    sf.max('player_total_kills').alias('kills'),
    sf.max('player_total_deaths').alias('deaths'),
    sf.max('player_total_assists').alias('assists')
).cache() 

## Most average kills per match
most_average_kills_per_match = (
    match_players_stats
    .groupBy('player_gamertag')
    .agg(
        sf.avg('kills').alias('avg_kills')
    )
    .sort(sf.desc('avg_kills'))
    .take(1)
)
print(f'Most average kills per match: {most_average_kills_per_match[0]['player_gamertag']} with {most_average_kills_per_match[0]['avg_kills']}')

## Most played playlist
most_played_playlist = (
    match_players_stats
    .groupBy('playlist_id')
    .agg(
        sf.count('playlist_id').alias('count')
    )
    .sort(sf.desc('count'))
    .take(1)
)
print(f'Most played playlist: {most_played_playlist[0]["playlist_id"]}')

## Most played map
most_played_map = (
    match_players_stats.alias('m')
    .join(
        spark.table('bootcamp.maps').alias('mp'),
        sf.col('m.map_id') == sf.col('mp.mapid')
    )
    .groupBy('map_id')
    .agg(
        sf.max('name'),
        sf.count('map_id').alias('count')
    )
    .sort(sf.desc('count'))
    .take(1)
)
print(f'Most played map: {most_played_map[0]["max(name)"]} with {most_played_map[0]["count"]}')

## Map with most Killing Sprees by players
most_killing_spree_map = (
    match_player_medals
    .join(
        sf.broadcast(spark.table('bootcamp.medals')),
        'medal_id'
    )
    .alias('d')
    .join(
        sf.broadcast(spark.table('bootcamp.maps')).alias('m'),
        'mapid'
    )
    .where('d.name = "Killing Spree"')
    .groupBy('mapid')
    .agg(
        sf.max('m.name').alias('map_name'),
        sf.max('d.name').alias('medal_name'),
        sf.sum('count').alias('count')
    )
    .sort(sf.desc('count'))
    .take(1)
)
print(f'Map with most Killing Sprees by players: {most_killing_spree_map[0]["map_name"]} with {most_killing_spree_map[0]["count"]}')

## No sorting with bucketing by match_id
(
    match_player_medals
    .writeTo('bootcamp.match_player_models')
    .tableProperty('write.bucket-by-columns', 'match_id')
    .tableProperty('write.bucket-by-number', '16')
    .createOrReplace()
)
spark.table('bootcamp.match_player_models.files').select(sf.sum('file_size_in_bytes')).show()

## Sorting via mapid
(
    match_player_medals
    .sort('mapid')
    .writeTo('bootcamp.match_player_models')
    .tableProperty('write.bucket-by-columns', 'match_id')
    .tableProperty('write.bucket-by-number', '16')
    .createOrReplace()
)
spark.table('bootcamp.match_player_models.files').select(sf.sum('file_size_in_bytes')).show()

## Sort via playlist_id
(
    match_player_medals
    .sort('playlist_id')
    .writeTo('bootcamp.match_player_models')
    .tableProperty('write.bucket-by-columns', 'match_id')
    .tableProperty('write.bucket-by-number', '16')
    .createOrReplace()
)
spark.table('bootcamp.match_player_models.files').select(sf.sum('file_size_in_bytes')).show()

## Sort via mapid and playlist_id
(
    match_player_medals
    .sort('mapid', 'playlist_id')
    .writeTo('bootcamp.match_player_models')
    .tableProperty('write.bucket-by-columns', 'match_id')
    .tableProperty('write.bucket-by-number', '16')
    .createOrReplace()
)
spark.table('bootcamp.match_player_models.files').select(sf.sum('file_size_in_bytes')).show()

spark.stop()