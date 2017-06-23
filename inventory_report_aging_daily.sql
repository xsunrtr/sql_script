/*get the next updated_on and location_station for each seq(action)*/
DROP TABLE IF EXISTS inventory_unit_next_action;
create table inventory_unit_next_action as 
select  iu.id
,iu.seq
,iu.status
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
DROP TABLE IF EXISTS inventory_unit_next_action_over48;
create table inventory_unit_next_action_over48 as 
select inventory_unit_next_action.* 
, DATEDIFF(hour, inventory_unit_next_action.cur_updated_on, inventory_unit_next_action.next_updated_on) as time_to_next_scan
from inventory_unit_next_action
where next_updated_on >= (cur_updated_on+ interval '48 HOUR')
and inventory_unit_next_action.next_updated_on >='2017-01-01'
and status <> 'INACTIVE';


/*Join rtrbi.dates table to get each single date within aging period of each unit*/
DROP TABLE IF EXISTS aging_report_table;
create table aging_report_table as 
select inventory_unit_next_action_over48.*
,dates.fiscal_week
,dates.fiscal_week_start
,dates.fiscal_week_end
,dates.asofdate
from inventory_unit_next_action_over48
inner join rtrbi.dates as dates
on (dates.asofdate + interval '10 HOUR') >= inventory_unit_next_action_over48.aging_date
and (dates.asofdate + interval '10 HOUR') <= inventory_unit_next_action_over48.next_updated_on
where cur_location_station not in ('CUSTOMER','RACK','DARK_STORAGE')
and asofdate < CURRENT_DATE
and (regexp_like(barcode, '^9') or regexp_like(barcode, '^1')) ;



/*Summarize target table by asofdate, location_station and aging days bins*/
select 
count(*) as total,
count(CASE WHEN time_to_next_scan>=48 AND time_to_next_scan < 72 THEN 1 END) AS '2-3 days',
count(CASE WHEN time_to_next_scan>=72 AND time_to_next_scan<240 THEN 1 END) AS '3-10 days',
count(CASE WHEN time_to_next_scan>=240 AND time_to_next_scan<720 THEN 1 END) AS '10-30 days',
count(CASE WHEN time_to_next_scan>=720 AND time_to_next_scan<4320 THEN 1 END) AS '30-180 days',
count(CASE WHEN time_to_next_scan>=4320 THEN 1 END) AS '180 days and above'
,asofdate,cur_location_site
from aging_report_table
where asofdate is not null
group by asofdate,cur_location_site
order by asofdate DESC;





drop table if exists inventory_unit_next_action;
drop table if exists inventory_unit_next_action_over48;