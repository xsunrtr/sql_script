/*get the next updated_on and location_station for each seq(action)*/
DROP TABLE IF EXISTS temp;
create table temp as 
select  iu.id
,iu.seq
,iu.barcode
,iu.updated_on as cur_updated_on
,coalesce(LEAD(iu.updated_on) OVER(PARTITION BY iu.id order by iu.seq),getdate()) as next_updated_on
,iu.action as cur_action
,LEAD(iu.action) OVER(PARTITION BY iu.id order by iu.seq) as next_action
,iu.location_site as cur_location_site
,iu.location_station as cur_location_station
,LEAD(iu.location_station) OVER(PARTITION BY iu.id order by iu.seq) as next_location_station
,(iu.updated_on+ interval '48 HOUR') as aging_date
from etl.inventory_unit as iu;

/*Downsize target table with filters on location_site, aging over 2 days and updated_on date*/
DROP TABLE IF EXISTS temp1;
create table temp1 as 
select temp.* 
, DATEDIFF(hour, temp.cur_updated_on, temp.next_updated_on) as time_to_next_scan
from temp
left join etl.inventory_unit_seq as ius
on temp.id = ius.unit_id
where cur_location_site not in ('CUSTOMER','RACK') 
and next_updated_on >= (cur_updated_on+ interval '48 HOUR')
and temp.cur_updated_on >='2017-01-01';

/*Join rtrbi.dates table to get each single date within aging period of each unit*/
DROP TABLE IF EXISTS temp2;
create table temp2 as 
select temp1.*
,dates.fiscal_week
,dates.fiscal_week_start
,dates.fiscal_week_end
,dates.asofdate
from temp1
left join rtrbi.dates as dates
on (dates.asofdate + interval '10 HOUR') >= temp1.aging_date
and (dates.asofdate + interval '10 HOUR') <= temp1.next_updated_on;

select distinct(time_to_next_scan) from temp2 order by time_to_next_scan ASC;
/*Summarize target table by asofdate, location_station and aging days bins*/
select 
count(*),
count(CASE WHEN time_to_next_scan>=48 AND time_to_next_scan < 72 THEN 1 END) AS '2-3 days',
count(CASE WHEN time_to_next_scan>=72 AND time_to_next_scan<240 THEN 1 END) AS '3-10 days',
count(CASE WHEN time_to_next_scan>=240 AND time_to_next_scan<720 THEN 1 END) AS '10-30 days',
count(CASE WHEN time_to_next_scan>=720 AND time_to_next_scan<4320 THEN 1 END) AS '30-180 days',
count(CASE WHEN time_to_next_scan>=4320 THEN 1 END) AS '180 days and above'
,asofdate,cur_location_station
from temp2
where asofdate is not null
group by asofdate,cur_location_station
order by asofdate,cur_location_station;

drop table if exists temp;
drop table if exists temp1;



