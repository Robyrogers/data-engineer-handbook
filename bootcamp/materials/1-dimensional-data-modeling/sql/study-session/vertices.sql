create type vertex_type as 
	enum ('player', 'team', 'game');

create table vertices (
	identifier text,
	type vertex_type,
	properties json,
	primary key(identifier, type)
);