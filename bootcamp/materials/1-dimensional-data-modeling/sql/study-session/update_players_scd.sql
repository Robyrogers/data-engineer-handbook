insert into players_scd 
with last_season_scd as (
	select * from players_scd
	where current_season = 2021 and
		end_season = 2021
), historical_scd as (
	select player_name,
		scoring_class,
		is_active,
		start_season,
		end_season,
		2022 as current_season
	from players_scd
	where current_season = 2021 and
		end_season < 2021
), this_season as (
	select * from players
	where current_season = 2022
), unchanged_records as (
	select 
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ts.current_season as end_season,
		2022 as current_season
	from this_season ts
	join last_season_scd ls
	on ts.player_name = ls.player_name
	where ts.scoring_class = ls.scoring_class
	and ts.is_active = ls.is_active
), changed_records as (
	select 
		ts.player_name,
		(unnest(array[
			row(
				ls.scoring_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
			)::scd_type,
			row(
				ts.scoring_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
			)::scd_type
		])::scd_type).*,
		2022 as current_season
	from this_season ts
	join last_season_scd ls
	on ts.player_name = ls.player_name
	where ts.scoring_class <> ls.scoring_class
	or ts.is_active <> ls.is_active
), new_records as (
	select 
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ts.current_season as start_season,
		ts.current_season as end_season,
		2022 as current_season
	from this_season ts
	join last_season_scd ls
	on ts.player_name = ls.player_name
	where ls.player_name is null
)
select * from historical_scd
union all
select * from unchanged_records
union all 
select * from changed_records
union all
select * from new_records;