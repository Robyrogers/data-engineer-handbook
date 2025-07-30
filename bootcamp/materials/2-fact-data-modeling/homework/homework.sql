-- Query 1
with deduped as (
	select *, row_number() over (partition by game_id, team_id, player_id) as row_num
	from game_details
)
select * from deduped where row_num = 1;

-- Query 2
create table user_devices_cumulated (
	user_id numeric,
	device_id numeric,
	browser_type text,
	device_activity_datelist Date[],
	primary key (user_id, device_id, browser_type)
);

-- Query 3
insert into user_devices_cumulated 
with deduped_devices as (
	select *, row_number() over (partition by device_id) as row_num
	from devices
), activity as (
	select 
		e.user_id as user_id, 
		e.device_id as device_id,
		d.browser_type as browser_type,
		max(e.event_time::Date) as event_date 
	from events e inner join
	deduped_devices d on e.device_id = d.device_id
	where row_num = 1 and user_id is not null
	group by e.user_id, e.device_id, d.browser_type, e.event_time::Date
)
select 
	user_id,
	device_id,
	browser_type,
	array_agg(event_date) as device_activity_datelist
from activity
group by user_id, device_id, browser_type
;

-- Query 4
with dates as (
	select generate_series(min(event_time::Date), max(event_time::Date), '1 day'::interval)::Date as activity_date from events
), activity as (
	select
		*, array[d.activity_date] <@ u.device_activity_datelist as is_active from dates d
	cross join user_devices_cumulated u 
)
select 
	user_id,
	device_id,
	browser_type,
	date_trunc('month', activity_date)::Date as start_date,
	sum(
		case 
			when is_active then power(2, 31 - (activity_date - date_trunc('month', activity_date)::Date))
			else 0
		end
	)::bigint::bit(32) as date_activity_int
from activity
group by user_id, device_id, browser_type, date_trunc('month', activity_date)
;

-- Query 5
create table host_cumulated (
	host text,
	host_activity_datelist Date[],
	primary key (host)
);

-- Query 6
insert into host_cumulated
with activity as (
	select 
		host,
		event_time::Date as activity_date,
		count(1)
	from events
	where user_id is not null
	group by host, event_time::Date
)
select
	host,
	array_agg(activity_date) as host_activity_datelist
from activity
group by host
;

-- Query 7
create table host_activity_reduced (
	host text,
	month Date,
	hit_array integer[],
	unique_visitors integer[],
	primary key (host, month)
);

-- Query 8
insert into host_activity_reduced 
with yesterday as (
	select * from host_activity_reduced
), today as (
	select 
		host,
		event_time::Date as hit_date,
		count(1) as hits,
		count(distinct user_id) as unique_users
	from events 
	where event_time::Date = Date('2023-01-01') and user_id is not null
	group by host, event_time::Date
) 
select 
	coalesce(y.host, t.host) as host,
	coalesce(y.month, date_trunc('month', t.hit_date))::Date as month,
	case
		when y.host is not null and t.host is not null then y.hit_array || array[t.hits]
		when y.host is null then array[t.hits]
		else y.hit_array || array[0]
	end as hit_array,
	case
		when y.host is not null and t.host is not null then y.unique_visitors || array[t.unique_users]
		when y.host is null then array[t.unique_users]
		else y.unique_visitors || array[0]
	end as unique_visitors
from yesterday y
full join today t on y.host = t.host and y.month = date_trunc('month', t.hit_date)
on conflict (host, month)
do update set hit_array = excluded.hit_array, unique_visitors = excluded.unique_visitors
;