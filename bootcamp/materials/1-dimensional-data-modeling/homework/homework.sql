create type films_type as (
    film text,
    votes integer,
    rating real,
    film_id text
);

create type quality_type as enum (
    'star',
    'good',
    'average',
    'bad'
);

create table actors (
    actor_id text,
    actor_name text,
    current_year integer,
    is_active boolean,
    quality_class quality_type,
    films films_type[],
    primary key (actor_id, current_year)
);

-- Load into actors one year at a time
insert into actors
with prev_year as (
	select * from actors 
	where current_year = 1969
	order by actor_id, current_year
), current_year as (
	select 
		actorid as actor_id,
		max(actor) as actor_name,
		year as current_year,
		max(case 
			when af.filmid is not null
				then 1
				else 0
		end)::boolean as is_active,
		avg(rating) as average_rating,
		array_remove(array_agg(
			case 
				when af.filmid is not null
					then row(
						film,
						votes,
						rating,
						filmid
					)::films_type
			end
		), null) as films
	from actor_films af
	where year = 1970
	group by actorid, year
), with_quality as (
	select
		*,
		(case 
			when average_rating > 8 then 'star'
			when average_rating > 7 then 'good'
			when average_rating > 6 then 'average'
			else 'bad'
		end)::quality_type as quality_class	
	from current_year
), new_movies as (
	select 
		n.actor_id ,
		n.actor_name,
		n.current_year,
		n.is_active,
		n.quality_class,
		n.films
	from with_quality n
	left join prev_year p
	on p.actor_id = n.actor_id
), no_new_movies as (
	select 
		p.actor_id,
		p.actor_name,
		p.current_year + 1,
		false as is_active,
		p.quality_class,
		array[]::films_type[] as films
	from prev_year p
	left join with_quality n
	on p.actor_id = n.actor_id
	where n.actor_id is null
)
select * from new_movies
union all
select * from no_new_movies;

-- Backfill query for actors_history_scd
with prev as (
	select 
		*,
		lag(is_active, 1) over (partition by actor_id order by current_year) as prev_is_active,
		lag(quality_class, 1) over (partition by actor_id order by current_year) as prev_quality_class
	from actors where current_year <= 2020
), with_change_indicator as (
	select 
		*,
		case 
			when (prev_is_active is null or prev_quality_class is null) then 0
			else (prev_is_active <> is_active or prev_quality_class <> quality_class)::integer
		end	as change_indicator
	from prev
), with_change_streak as (
	select
		*,
		sum(change_indicator) over (partition by actor_id order by current_year) as change_streak
	from with_change_indicator 
)
select
	actor_id ,
	max(actor_name),
	max(quality_class),
	max(is_active::integer)::boolean as is_active,
	min(current_year) as start_year,
	max(current_year) as end_year,
	2020 as current_year
from with_change_streak 
group by actor_id , change_streak ;

-- Incremental query for actors_history_scd
insert into actors_history_scd 
with prev as (
	select * from actors_history_scd where current_year = 2020
), current_year as (
	select *
	from actors
	where current_year = 2021
), new_actors as (
	select 
		c.actor_id,
		c.actor_name,
		c.quality_class,
		c.is_active,
		c.current_year as start_year,
		c.current_year as end_year,
		c.current_year
	from current_year c
	left join prev p
	on c.actor_id = p.actor_id	
	where p.actor_id is null
), unchanged_entries as (
	select 
		p.actor_id,
		p.actor_name,
		p.quality_class,
		p.is_active ,
		p.start_year,
		p.end_year,
		2021 as current_year
	from prev p
	where p.end_year < 2020
), no_new_movies as (
	select 
		c.actor_id,
		c.actor_name,
		c.quality_class,
		c.is_active,
		case
			when p.is_active <> c.is_active then c.current_year
			else p.start_year
		end as start_year,
		p.end_year + 1 as end_year,
		p.current_year + 1 as current_year
	from prev p
	join current_year c
	on p.actor_id = c.actor_id
	where p.end_year = 2020 and c.is_active = false
), new_movies as (
	select 
		c.actor_id,
		c.actor_name,
		c.quality_class,
		c.is_active,
		case
			when (p.is_active <> c.is_active or p.quality_class <> c.quality_class) then c.current_year
			else p.start_year
		end as start_year,
		p.end_year + 1 as end_year,
		p.current_year + 1 as current_year
	from prev p
	join current_year c
	on p.actor_id = c.actor_id
	where p.end_year = 2020 and c.is_active = true
)
select * from unchanged_entries 
union all
select * from no_new_movies 
union all
select * from new_movies
union all
select * from new_actors;