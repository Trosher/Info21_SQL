--1ex
create or replace procedure embedding_in_P2P(CheñkPeer_new varchar, CheckingPeer_new varchar,
											NameTask varchar, State_new check_status,
											TimeCheck time) language plpgsql as $$
	declare state_last check_status;
	declare p_check_id bigint; -- 1
	begin
		select max(p."ID") into p_check_id 
		from P2P p
		join Checks c on c."ID" = p."Check"
		where p."CheckingPeer" = CheckingPeer_new and 
		c."Peer" = CheñkPeer_new and c."Task" = NameTask;
		select p."State" into state_last from P2P p 
		where p."ID" = p_check_id;
		if (State_new != state_last and ((State_new != 'success' and state_last != 'failure')
		   or (State_new != 'failure' and state_last != 'success')) or (state_last is null and State_new = 'start')) then
			if (State_new = 'start') then
				insert into Checks ("Peer", "Task", "Date")
				values (CheñkPeer_new, NameTask, current_date);
				insert into P2P ("Check", "CheckingPeer", "State", "Time")
				values ((select max("ID") from Checks), CheckingPeer_new, State_new, TimeCheck);
			else
				insert into P2P ("Check", "CheckingPeer", "State", "Time")
				values ((select "Check" from p2p where "ID" = p_check_id), CheckingPeer_new, State_new, TimeCheck);
			end if;
		end if;
	end;
$$;

--2ex
create or replace procedure embedding_in_Verter(CheñkPeer_new varchar, NameTask varchar,
											   State_new check_status, TimeCheck time) language plpgsql as $$
	declare max_id_p bigint;
	declare verter_id bigint;
	declare check_id bigint;
	declare state_last check_status;
	begin
		select max(pp."ID") into max_id_p from checks c
		join p2p pp on pp."Check" = c."ID"
		and c."Peer" = CheñkPeer_new
		and c."Task" = NameTask
		and pp."State" = 'success';
		if (max_id_p is not null) then
			select max("ID") into verter_id from verter
			where "Check" = (select distinct "Check"
							 from p2p
							 where "ID" = max_id_p);
			select "State" into state_last from verter
			where "ID" = verter_id;
			if ((state_last is null and State_new = 'start') or
				(State_new != state_last and ((State_new != 'success' and state_last != 'failure')
		   		or (State_new != 'failure' and state_last != 'success')))) then
		   		select "Check" into check_id from p2p
		   		where "ID" = max_id_p;
				insert into Verter ("Check", "State", "Time")
				values (check_id, State_new, TimeCheck);
			end if;
		end if;
	end;
$$;

--3ex
create or replace function func_transfer() returns trigger as $func_transfer$
    declare checking_peer varchar;
    declare checked_peer varchar;
    begin
        checking_peer:= new."CheckingPeer";
        select "Peer" into checked_peer from Checks where new."Check" = checks."ID";
        if (new."State" = 'start') then
            if (exists(select "CheckingPeer", "CheckedPeer" from transferredpoints where
                "CheckedPeer" = checked_peer and "CheckingPeer" = checking_peer)) then
                update transferredpoints
                set "PointsAmount" = "PointsAmount" + 1
                where "CheckingPeer" = new."CheckingPeer" and "CheckedPeer" = checked_peer;
            elseif (exists(select "ID" from transferredpoints) = true) then
                insert into transferredpoints
                values ((select max("ID") from transferredpoints) + 1,
                        checking_peer, checked_peer, 1);
            else
                insert into transferredpoints
                values (1, checking_peer, checked_peer, 1);
            end if;
        end if;
        return new;
    end;
$func_transfer$ language plpgsql;

create or replace trigger transfer_pts
after insert on P2P
for each row
execute function func_transfer();

--4ex
create or replace function is_check_successfull(check_id bigint) returns bool as $is_check_successfull$
    declare result bool;
    begin
        if ((select "State" from p2p where "Check" = check_id and "ID" = (select max("ID") from p2p)) = 'success' and
            ((select "State" from Verter where "Check" = check_id and "ID" = (select max("ID") from Verter)) = 'success' or 
            exists(select "State" from Verter where "Check" = check_id) = false)) then
            result:= true;
        else
            result:=false;
        end if;
        return result;
    end;
$is_check_successfull$ language plpgsql;

create or replace function check_xp() returns trigger as $check_xp$
    declare max_xp int;
    declare is_successful bool;
    begin
        is_successful:= is_check_successful((select "ID" from Checks where new."Check" = "ID"));
        max_xp:= (select Tasks."MaxXP" from Checks join Tasks on Tasks."Title" = Checks."Task" and Checks."ID" = new."Check");
        if (new."XPAmount" <= max_xp or is_successful = true) then
            insert into XP values (new."ID", new."Check", new."XPAmount");
        end if;
        return new;
    end;
$check_xp$ language plpgsql;

create or replace trigger check_xp_trigger
before insert on XP
execute function check_xp();
