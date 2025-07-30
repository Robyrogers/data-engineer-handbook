insert into vertices
select
	game_id as identifier,
	'game'::vertex_type as type,
	json_build_object(
		'pts_home', pts_home ,
		'pts_away', pts_away,
		'winning_team', case
			when home_team_wins = 1 then home_team_id else visitor_team_id  
		end
	) as properties
from games;


insert into vertices
with players_agg as (
	select
		player_id as identifier,
		max(player_name ) as player_name ,
		count(1) as number_of_games,
		sum(pts ) as total_points,
		array_agg(distinct team_id) as teams
	from game_details
	group by player_id
)
select
	identifier,
	'player'::vertex_type,
	json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games ,
		'total_points', total_points ,
		'teams', teams
	)
from players_agg ;


insert into vertices 
with teams_deduped as (
	select *, row_number() over(partition by team_id) as row_num
	from teams
)
select
	team_id as identifier,
	'team'::vertex_type as type,
	json_build_object(
		'abbreviation', abbreviation ,
		'nickname', nickname ,
		'city', city ,
		'arena', arena ,
		'year_founded', yearfounded 
	)
from teams_deduped 
where row_num = 1;