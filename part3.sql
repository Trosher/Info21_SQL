-- ex1
create or replace function transferredpoints_hr()
returns table("Peer1" varchar, "Peer2" varchar, "PointsAmount" int) as $show_transferred_points$
    begin
		return query
	  	select tp1."CheckingPeer" as peer1,
    		tp1."CheckedPeer" as peer2,
    		tp1."PointsAmount" - coalesce(tp2."PointsAmount", 0)
    	as pointsamount
  		from transferredpoints tp1
  		left join transferredpoints tp2 on tp1."CheckedPeer" = tp2."CheckingPeer"
  		and tp1."CheckingPeer" = tp2."CheckedPeer"
  		order by pointsamount desc;
    end;
$show_transferred_points$ language plpgsql;

select * from transferredpoints_hr();

--ex2
create or replace function fnc_show_successful_tasks()
returns table ("Peer" varchar, "Task" varchar, "XP" int) as $fnc_show_successful_tasks$
    begin
        return query
            select checks."Peer", checks."Task", xp."XPAmount"
            from checks
            join xp on checks."ID" = xp."Check";
    end;
$fnc_show_successful_tasks$ language plpgsql;

select * from fnc_show_successful_tasks();

--ex3
create or replace function fnc_did_not_left_campus(needed_date date)
returns setof varchar as $fnc_did_not_left_campus$
    begin
        return query
            select p."Nickname"
            from peers p
            join timetracking t on p."Nickname" = t."Peer"
            where t."Date" = $1 and t."State" = 1
            except
            select distinct p."Nickname"
            from peers p
            join timetracking t on p."Nickname" = t."Peer"
            where t."Date" = $1 and t."State" = 2;
    end;
$fnc_did_not_left_campus$ language plpgsql;

select * from fnc_did_not_left_campus('12.05.2022');

--ex4
create or replace function fnc_checks_percentage() 
returns table (Peer varchar, PointsChange numeric) 
as $$
    begin
	    return query
	    SELECT subquery.Peer as Peer, SUM(subquery.PointsChange) AS PointsChange
		FROM (
		  SELECT TransferredPoints."CheckingPeer" AS Peer, 
		  SUM(TransferredPoints."PointsAmount") AS PointsChange
		  FROM TransferredPoints
		  GROUP BY TransferredPoints."CheckingPeer"
		  UNION ALL
		  SELECT TransferredPoints."CheckedPeer" AS Peer, 
		  -SUM(TransferredPoints."PointsAmount") AS PointsChange
		  FROM TransferredPoints
		  GROUP BY TransferredPoints."CheckedPeer"
		) AS subquery
		GROUP BY subquery.Peer
		ORDER BY PointsChange DESC;
  	end;
$$ language plpgsql;

select * from fnc_checks_percentage();

--ex5

create or replace function CalculatePointsForGeeks() 
returns table (Peer varchar, PointsChange numeric)
as $$
	begin
	    return query
	    SELECT subquery.Peer as Peer, SUM(subquery.PointsChange) AS PointsChange
		FROM (
		  SELECT "Peer1" AS Peer, 
		  SUM("PointsAmount") AS PointsChange
		  FROM transferredpoints_hr()
		  GROUP BY "Peer1"
		  UNION ALL
		  SELECT "Peer2" AS Peer, 
		  -SUM("PointsAmount") AS PointsChange
		  FROM transferredpoints_hr()
		  GROUP BY "Peer2"
		) AS subquery
		GROUP BY subquery.Peer
		ORDER BY PointsChange DESC;
  	end;
$$ language plpgsql;

select * from CalculatePointsForGeeks();

--ex6

create or replace function FindMaxCheckedTask()
returns table (Date date, Task varchar)
language plpgsql
AS $$
  begin
	return query
    SELECT "Date", "Task"
	FROM checks
	GROUP BY "Date", "Task"
	HAVING COUNT(*) > 1;
  end;
$$;

select * from FindMaxCheckedTask();

--ex7

create or replace function FindAllPeersForFinalBlock(task_name_pp varchar) 
returns table (Nikname varchar, Date date)
language plpgsql
as $$
  begin
	return query
    select p."Nickname", c."Date" from verter vr
    join checks c on c."ID" = vr."Check"
    join peers p on p."Nickname" = c."Peer"
    where vr."State" = 'success' and
    c."Task" like case
      when task_name_pp like 'CPP' then 'CPP5%'
      when task_name_pp like 'C' then 'C8'
      when task_name_pp like 'DO' then 'DO6'
      when task_name_pp like 'A' then 'A8'
      when task_name_pp like 'SQL' then 'SQL3'
    end;
  end;
$$;

select * from FindAllPeersForFinalBlock('SQL');

--ex8

create or replace function RecomendPeer()
returns table (Peer varchar, RecommendedPeer varchar)
language plpgsql
as $$
	begin
		return query
		SELECT p1."Peer1" AS "Peer", p1."Peer2" AS RecommendedPeer
		FROM (
		    SELECT p1."Peer1", p2."Peer2", COUNT(*) AS RecommendationCount
		    FROM friends p1
		    INNER JOIN friends p2 ON p1."Peer2" = p2."Peer1"
		    GROUP BY p1."Peer1", p2."Peer2"
		) AS recommendations
		INNER JOIN friends p1 ON recommendations."Peer1" = p1."Peer1"
		INNER JOIN (
		    SELECT "Peer2", MAX(RecommendationCount) AS MaxCount
		    FROM (
		        SELECT p1."Peer1", p2."Peer2", COUNT(*) AS RecommendationCount
		        FROM friends p1
		        INNER JOIN friends p2 ON p1."Peer2" = p2."Peer1"
		        GROUP BY p1."Peer1", p2."Peer2"
		    ) AS subquery
		    GROUP BY "Peer2"
		) AS max_counts ON recommendations."Peer2" = max_counts."Peer2";
	end;
$$;

select * from RecomendPeer();

--ex9

create or replace procedure PercentPeerSuccessPassedBlock(
    block1 varchar,
    block2 varchar
)
language plpgsql
as $$
  declare
    start_block1 int;
    start_block2 int;
    start_both_block int;
    not_start_empty_block int;
  BEGIN
    start_block1 :=
      (select count(counts) from
        (select "Peer", "Task", count(distinct "Task") as counts
        from checks
        group by "Task", "Peer") as tb1
       where tb1."Task" = block1
        group by counts) * 100/ (select count(*)from peers);
    start_block2 :=
      (select count(counts) from
        (select "Peer", "Task", count("Task") as counts from checks
        group by "Task", "Peer") as tb1
      where tb1."Task" = block2
      group by counts) * 100/ (select count(*) from peers);
    start_both_block :=
      (select count(counts) from
        (select "Peer", "Task", count("Task") as counts from checks
        group by "Task", "Peer") as tb1
      where tb1."Task" in (block1, block2)
      group by counts) * 100/ (select count(*) from peers);
    not_start_empty_block :=
      (select avg(counts) * 100 / (select count(*) from peers)
	    from 
	    (
	        select "Peer", "Task", count("Task") as counts 
	        from checks
	        group by "Task", "Peer"
	    ) as tb1
	    where tb1."Task" not in (block1, block2));
  end;
$$;

call PercentPeerSuccessPassedBlock('SQL', 'A');

--ex10

create or replace function Delivery_on_your_birthday()
returns table ("SuccessfulChecks" int, "UnsuccessfulChecks" int)
as $Delivery_on_your_birthday$
begin
	return query
		with aga(State) as
		(
			select v."State" from checks c
			join verter v on v."Check" = c."ID" and v."State" != 'start'
			union all
			select pp."State" 
			FROM p2p pp 
			where pp."State" = 'failure'
		),
		SS(SuccessfulChecks) as 
		(
			select count(a.State) as SuccessfulChecks from aga a
			where a.State = 'success'
		),
		UU(UnsuccessfulChecks) as 
		(
			select count(a.State) as UnsuccessfulChecks from aga a
			where a.State = 'failure'
		)
		select (s.SuccessfulChecks::numeric / count(a.State)::numeric * 100)::int as SuccessfulChecks,
			   (u.UnsuccessfulChecks::numeric / count(a.State)::numeric * 100)::int as UnsuccessfulChecks
		from SS s, UU u, aga a
		GROUP BY s.SuccessfulChecks, u.UnsuccessfulChecks;
end;
$Delivery_on_your_birthday$ LANGUAGE plpgsql;

select * from Delivery_on_your_birthday();

--ex11

CREATE OR REPLACE FUNCTION GetPeerPassedTask(
	task_1 varchar,
	task_2 varchar,
	task_3 varchar
)
RETURNS TABLE(Peer varchar)
AS $$
	begin
		RETURN QUERY
		select "Peer" from checks
		where "Task" in (task_1, task_2) and "Task" <> task_3;
	end; 
$$ LANGUAGE plpgsql;

select * from GetPeerPassedTask('SQL01', 'SQL00', 'SQL02');

--ex12

create or replace function RecursiveCountTaskInBlock()
  returns table(task varchar, prevcount int)
as $$
begin
	RETURN QUERY
	WITH RECURSIVE TaskCTE AS (
    SELECT "Title", "ParentTask", 0 AS PrevCount
    FROM tasks
    WHERE "ParentTask" IS NULL
    UNION ALL
    SELECT t."Title", t."ParentTask", tc.PrevCount + 1
    FROM tasks t
    INNER JOIN TaskCTE tc ON t."ParentTask" = tc."Title"
	)
	SELECT "Title", TaskCTE.PrevCount
	FROM TaskCTE;
end;
$$ language plpgsql;

select * from RecursiveCountTaskInBlock();

--ex13

create or replace procedure LuckyDaysForChecks(
    n_count int
)
language plpgsql
as $$
  begin
    create materialized view list_all_peers as (
      select ch.peer, ch.task, ch.date, p.status, row_number()
      over (partition by p.time order by p.status) as row_number
      from checks ch
      join p2p p on ch.id = p.checkid
      where status <> 'Start'
    );
    select date from list_all_peers where row_number = n_count and status = 'Success';
  end;
$$;

--ex14
create or replace function All_Xp_fro_peer()
returns table ("Peer" varchar, "XP" bigint)
as $All_Xp_fro_peer$
begin
	return query
		with all_add_xp(Peers, XP) as
		(
			select c."Peer", sum(x."XPAmount") from xp x 
			join checks c on x."Check" = c."ID"
			group by c."Peer"
		)
		select a.Peers, a.XP from all_add_xp a
		where a.XP = (select max(xp) from all_add_xp);
end;
$All_Xp_fro_peer$ LANGUAGE plpgsql;

select * from All_Xp_fro_peer();

--ex15

create or replace function PunctualPeers(
    time_vis time,
    n_count int
)
RETURNS TABLE(Peer varchar)
as $PunctualPeers$
  begin
	return query
    select "Peer" from (select "Peer", count("Peer") as count
    from timetracking where "Time" < time_vis and "State" = 1
    group by "Peer") as foo
    where count >= n_count;
  end;
$PunctualPeers$ language plpgsql;

select * from PunctualPeers('09:00:00', 1);

--ex16

create or replace function PeersDay(
    m_count int,
    n_count int
)
RETURNS TABLE(Peer varchar)
as $PeersDay$
  begin
	return query
    select truant."Peer" from
    (select "Peer", count("Peer") from timetracking
    where "Date" between current_date - n_count
    and current_date
    and "State" = 1
    group by timetracking."Peer") as truant
    where truant.count >= m_count;
  end;
$PeersDay$ language plpgsql;

select * from PeersDay(1, 1000);

--ex17

create or replace function get_month_name(month_num numeric) returns text
as $get_month_name$
  declare
    month_name text;
  begin
    case month_num
      when 1 then month_name := 'january';
      when 2 then month_name := 'february';
      when 3 then month_name := 'march';
      when 4 then month_name := 'april';
      when 5 then month_name := 'may';
      when 6 then month_name := 'june';
      when 7 then month_name := 'july';
      when 8 then month_name := 'august';
      when 9 then month_name := 'september';
      when 10 then month_name := 'october';
      when 11 then month_name := 'november';
      when 12 then month_name := 'december';
      else month_name := 'invalid month number';
    end case;
    return month_name;
  end;
$get_month_name$ language plpgsql;

create or replace function DetermiteForEachMonthThePercentageOfEarlyEntries()
returns table (Month text, EarlyEntries bigint)
as $DetermiteForEachMonthThePercentageOfEarlyEntries$
  begin
    drop materialized view if exists table_nickname_entry_month_time_in_birthmonth;
    create materialized view table_nickname_entry_month_time_in_birthmonth as (
      select timetracking."Peer", extract(month from timetracking."Date") as entry_month, timetracking."Time"
      from timetracking
      join peers on timetracking."Peer" = peers."Nickname"
      where extract(month from timetracking."Date") = extract(month from peers."Birthday")
      and timetracking."State" = 1
    );
    return query
    select get_month_name(tmp.entry_month) as month, (tmp2.count * 100 / tmp.count) as early_entries_percent
    from (
      select entry_month, count(*) as count
      from table_nickname_entry_month_time_in_birthmonth
      group by entry_month
    ) as tmp
    join (
      select entry_month, count(*) as count, (count(*) * 100 / (select count(*)
      from table_nickname_entry_month_time_in_birthmonth where "Time" <= '12:00:00')) as percent
      from table_nickname_entry_month_time_in_birthmonth where "Time" <= '12:00:00'
      group by entry_month
    ) as tmp2 on tmp2.entry_month = tmp.entry_month;
  end;
$DetermiteForEachMonthThePercentageOfEarlyEntries$ language plpgsql;

select * from DetermiteForEachMonthThePercentageOfEarlyEntries();
