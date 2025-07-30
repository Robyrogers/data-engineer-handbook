create type edge_type as 
enum(
	'plays_against',
	'shares_team',
	'plays_in',
	'plays_on');

create table edges (
	subject_identifier text,
	subject_type vertex_type,
	object_identifier text,
	object_type vertex_type,
	edge_type edge_type,
	properties json,
	primary key(
		subject_identifier,
		subject_type,
		object_identifier,
		object_type,
		edge_type
	)
);