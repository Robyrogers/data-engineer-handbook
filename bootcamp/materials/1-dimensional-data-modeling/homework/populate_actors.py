# /// script
# dependencies = [
#   "psycopg[binary]"
# ]
# ///

import psycopg

with psycopg.connect("postgresql://postgres:postgres@localhost:5432/postgres") as conn:
    with conn.cursor() as cur:
        cur.execute("select generate_series(min(year), max(year)) as year from actor_films")
        years = cur.fetchall()

        for cur_year, in years:
            cur.execute("""
                insert into actors
                with prev_year as (
                    select * from actors 
                    where current_year = %s
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
                    where year = %s
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
                select * from no_new_movies
            """, (cur_year - 1, cur_year))