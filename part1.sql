create table Peers (
    "Nickname" varchar not null,
    "Birthday" date not null,
    primary key ("Nickname")
);

create table Tasks (
    "Title" varchar not null,
    "ParentTask" varchar,
    "MaxXP" int,
    primary key ("Title"),
    foreign key ("ParentTask") references Tasks("Title")
);

create type check_status as enum ('start', 'success', 'failure');

create table Checks (
    "ID" bigserial not null,
    "Peer" varchar not null,
    "Task" varchar not null,
    "Date" date not null,
    primary key ("ID"),
    foreign key ("Peer") references Peers("Nickname"),
    foreign key ("Task") references Tasks("Title")
);

create table P2P (
    "ID" bigserial not null,
    "Check" bigint not null,
    "CheckingPeer" varchar not null,
    "State" check_status not null,
    "Time" time without time zone not null,
    primary key ("ID"),
    foreign key ("Check") references Checks("ID"),
    foreign key ("CheckingPeer") references Peers("Nickname")
);

create table Verter (
    "ID" bigserial not null,
    "Check" bigint not null,
    "State" check_status not null,
    "Time" time without time zone not null,
    primary key ("ID"),
    foreign key ("Check") references Checks("ID")
);


create table TransferredPoints (
    "ID" bigserial not null,
    "CheckingPeer" varchar not null,
    "CheckedPeer" varchar not null,
    "PointsAmount" int not null,
    primary key ("ID"),
    foreign key ("CheckingPeer") references Peers("Nickname"),
    foreign key ("CheckedPeer") references Peers("Nickname")
);

create table Friends (
    "ID" bigserial not null,
    "Peer1" varchar not null,
    "Peer2" varchar not null,
    primary key ("ID"),
    foreign key ("Peer1") references Peers("Nickname"),
    foreign key ("Peer2") references Peers("Nickname")
);

create table Recommendations (
    "ID" bigserial not null,
    "Peer" varchar not null,
    "RecommendedPeer" varchar not null,
    primary key ("ID"),
    foreign key ("Peer") references Peers("Nickname"),
    foreign key ("RecommendedPeer") references Peers("Nickname")
);

create table XP (
    "ID" bigserial not null,
    "Check" bigint not null,
    "XPAmount" int not null,
    primary key ("ID"),
    foreign key ("Check") references Checks("ID")
);

create table TimeTracking (
    "ID" bigserial not null,
    "Peer" varchar not null,
    "Date" date not null,
    "Time" time without time zone,
    "State" int not null,
    primary key ("ID"),
    foreign key ("Peer") references Peers("Nickname"),
    constraint chk_state CHECK ("State" between 1 and 2)
);

create or replace function i_d(table_name varchar, sep char default ';') returns void as $$
    declare str text;
   	declare name_user text;
	begin
-- 		SELECT usename into name_user FROM pg_user limit(1);
        name_user:='zenaluth';
		str := 'copy ' || table_name || ' from ''/Users/' || name_user || 
		'SQL2_Info21_v1.0-0/src/imported_tables/'|| table_name  -- поставить имя для папки проекта aga_team
		||'.csv'' delimiter ''' || sep || ''' csv header';        
		execute (str);
	end;
$$ language plpgsql;

create or replace function import_db(sep char default ';') returns void as $$
    begin
        PERFORM i_d('Peers', sep);
		PERFORM i_d('Tasks', sep);
		PERFORM i_d('Checks', sep);
		PERFORM i_d('P2P', sep);
		PERFORM i_d('Verter', sep);
		PERFORM i_d('TransferredPoints', sep);
		PERFORM i_d('Friends', sep);
		PERFORM i_d('Recommendations', sep);
		PERFORM i_d('XP', sep);
		PERFORM i_d('TimeTracking', sep);
    end;
$$ language plpgsql;

create or replace function e_d(table_name varchar, sep char default ';') returns void as $$
    declare str text;
   	declare name_user text;
	begin
		-- SELECT usename into name_user FROM pg_user limit(1);
        name_user:='zenaluth';
		str := 'copy ' || table_name || ' to ''/Users/' || name_user || 
		'/SQL2_Info21_v1.0-0/src/export_tables/'|| table_name || -- поставить имя для папки проекта aga_team
		'.csv'' delimiter ''' || sep || ''' csv header';        
		execute (str);    
	end;
$$ language plpgsql;

create or replace function export_db(sep char default ';') returns void as $$
	begin
		PERFORM e_d('Peers', sep);
		PERFORM e_d('Tasks', sep);
		PERFORM e_d('Checks', sep);
		PERFORM e_d('P2P', sep);
		PERFORM e_d('Verter', sep);
		PERFORM e_d('TransferredPoints', sep);
		PERFORM e_d('Friends', sep);
		PERFORM e_d('Recommendations', sep);
		PERFORM e_d('XP', sep);
		PERFORM e_d('TimeTracking', sep);
	end;
$$ language plpgsql;
