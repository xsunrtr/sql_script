
--- create classic_rental table
drop table if exists rb_classic_rental;
create table rb_classic_rental as
select u.create_date as user_create_date
--, floor(DATEDIFF(day,u.dob_date,rb.creation_date)/365) as user_age
, case when rb.creation_date > uo.first_order_date then 0 else 1 end as new_flg
,uco.order_total 
,rbd.rental_begin_date
,rbd.rental_end_date
, first_value(ss.standard_size) over (partition by ss.psize order by ss.standard_size ASC) as standard_size
,pm.style_out
,pm.model
,pp.price
,rb.*
from etl.reservation_booking as rb
inner join etl.users as u
on u.uid = rb.uid and rb.uid>1
inner join etl.reservation_booking_detail as rbd
on rb.id = rbd.reservation_booking_id
left join rtrbi.order_group_reservation as ogr
on ogr.reservation_booking_id = rb.id
left join etl.uc_orders as uco
on uco.order_id = ogr.order_id
left join etl.products_master_iid as pm on rb.sku = pm.sku
left join (
select psize, size_scale, min(standard_size) as standard_size from etl.size_scales group by psize,size_scale) as ss 
on pm.size = ss.psize and pm.sizeScaleName=ss.size_scale
left join  rtrbi.product_prices as pp
on pm.model = pp.style_name and rb.creation_date BETWEEN pp.order_date_start and pp.order_date_end
and rbd.rental_begin_date BETWEEN pp.rent_begin_start and pp.rent_begin_end
left join (
select uid,
min(cast(to_timestamptz(created) at time zone 'US/EASTERN' as timestamptz)) as first_order_date
from etl.uc_orders
group by uid
) as uo
on rb.uid = uo.uid 
where rb.merchandise_category='RENTAL'  and pp.merchandise_category='RENTAL'
and day(rbd.rental_end_date-rbd.rental_begin_date) < 730
and rb.creation_date>='2016-05-01' and rb.creation_date<'2017-05-01';
--and not (rb.status=1 and  rb.type=2);


select count(*) from rb_classic_rental;

/*
 * Group classic rental data by user and RBD
 * */
drop table if exists sc_temp3;
create table sc_temp3 as
select rcr.uid,
d.fiscal_week_start as LPD_week_start,
count(*) as adding_to_SC,
count(distinct rcr.model) as style_count,
count(distinct rcr.sku) as sku_count,
min(distinct rcr.price) as min_price,
max(distinct rcr.price) as max_price,
avg(rcr.price) as cart_price,
--min(user_age) as user_age,
min(new_flg) as new_flg,
max(order_total) as order_total,
max(rcr.standard_size ) as max_size,
min(rcr.standard_size) as min_size,
floor(AVG(rcr.standard_size)) as avg_size,
sum(case rcr.type when 0 then 1 else null end) as placing_order,
min(creation_date) as first_addto_SC,
max(creation_date) as last_addto_SC,
sum(case when rcr.status=1 and  rcr.type=2 then 1 else 0 end) as cancel_rbs,
min(case when rcr.type = 0 then rcr.creation_date else null end) first_order,
max(case when rcr.type = 0 then rcr.creation_date else null end) last_order,
min(case when rcr.type = 0 then rcr.id else null end) reservation_booking_id
from rb_classic_rental as rcr
inner join rtrbi.dates as d on rcr.last_possible_begin_date=d.asofdate 
group by rcr.uid, d.fiscal_week_start
having uid>0 and uid <> 11141040;

select sc.*
,d.fiscal_week_start as first_addto_SC_week_start
,day(sc.LPD_week_start - d.fiscal_week_start)/7 as LPD_SC_dif_week
,(case when placing_order>0 then day(first_order - first_addto_SC) else null end) as order_SC_dif_day
,(case when placing_order>0 then 1 else 0 end) as classic_checkout_flag
,rb.booking_count as booking_count_classic_unlimited
from sc_temp3 as sc 
inner join rtrbi.dates as d on sc.first_addto_SC::DATE=d.asofdate
left join rb_event_temp as rb
on sc.uid = rb.uid and rb.LPD_week_start = sc.LPD_week_start
where first_addto_SC>='2016-08-01' and first_addto_SC<'2017-05-01';


select count(*) from sc_temp3;


drop table if exists rb_event_temp;
create table rb_event_temp as
select rb.uid 
,d.fiscal_week_start as LPD_week_start
,sum(case when rb.order_group_id>0 and rb.merchandise_category = 'RENTAL' then 1 else 0 end) as booking_count
from etl.reservation_booking as rb
inner join rtrbi.dates as d 
on rb.last_possible_begin_date =d.asofdate
group by rb.uid, d.fiscal_week_start;



select sc.*
,d.fiscal_week_start as first_addto_SC_week_start
,day(sc.LPD_week_start - d.fiscal_week_start)/7 as LPD_SC_dif_week
,(case when placing_order>0 then day(first_order - first_addto_SC) else null end) as order_SC_dif_day
,(case when placing_order>0 then 1 else 0 end) as classic_checkout_flag
,rb.booking_count as booking_count_classic_unlimited
from sc_temp3 as sc 
inner join rtrbi.dates as d on sc.first_addto_SC::DATE=d.asofdate
left join rb_event_temp as rb
on sc.uid = rb.uid and rb.LPD_week_start = sc.LPD_week_start
where first_addto_SC>='2016-08-01' and first_addto_SC<'2017-05-01';





/*
 * device
 * */
select * 
from rb_classic_rental limit 200;
--- 1. check if pixel_raw2 table has all rb data --- yes
drop table if exists temp;
create table temp as
select distinct 
rcr.* 
,pr.log_source
from rb_classic_rental as rcr
left join etl.products_master_iid as pm on rcr.sku = pm.sku
left join etl.pixel_raw2 as pr  on pm.model = pr.style_name 
and rcr.uid=pr.uid 
and rcr.rental_begin_date = pr.rent_begin_date::date
where action_type = 'rezo_add_to_cart' 
and rcr.uid>0 ;

select * from temp where log_source is null;

---2. multiple device by uid from sc_temp3
drop table if exists user_device;
create table user_device as
select distinct pr.uid,
d.device
from sc_temp3 as st
inner join etl.pixel_raw2 as pr on st.uid = pr.uid
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id
;

select ud.uid,
ud.device,
case when uo.first_order_date is null then 0 else 1 end as checkout_flag
from user_device as ud 
left join (
select uid,
min(cast(to_timestamptz(created) at time zone 'US/EASTERN' as timestamptz)) as first_order_date
from etl.uc_orders
group by uid
) as uo
on ud.uid = uo.uid
;


--- add device to sc_temp3 table (All devices were used for one shopping process)
drop table if exists visits_funnel_addtoSC_checkout;
create table visits_funnel_addtoSC_checkout as
select distinct d.device
,sc.*
from sc_temp3 as sc
left join etl.pixel_raw2 as pr 
on sc.uid=pr.uid
and pr.datetime_cst::date BETWEEN first_addto_SC::date
and (case when sc.first_order is not null then sc.last_order::date else sc.last_addto_SC::date end)
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id;

--- Do people switch device to checkout the same item?

drop table if exists device_checkout;
create table device_checkout as
select rb.sku
, d.device
,ogr.rental_begin_date
,ogr.uid
from rtrbi.browser_orders as bo
inner join rtrbi.order_group_reservation as ogr
on ogr.order_id=bo.order_id
inner join etl.reservation_booking as rb
on rb.id = ogr.reservation_booking_id
inner join rtrbi.browser_device as bd on bo.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id
where ogr.rental_begin_date >= '2016-05-01' ;


drop table if exists device_checkout_samesku;
create table device_checkout_samesku as
select dc.uid
,dc.rental_begin_date
,dc.sku
,dc.device as checkout_device
,d.device as add_to_cart_device
from etl.pixel_raw2 as pr
inner join etl.products_master_iid as pmi
on pr.style_name = pmi.model
inner join device_checkout as dc
on dc.sku = pmi.sku
and pr.uid = dc.uid and pr.rent_begin_date::date =dc.rental_begin_date
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id
where pr.action_type = 'rezo_add_to_cart';

select * from device_checkout_samesku limit 200;

select count(*) from device_checkout_samesku;
select count(*) from device_checkout_samesku where checkout_device<>add_to_cart_device;

select * from device_checkout_samesku;

----- device start vs end in one rental process

drop table if exists visits_funnel_addtoSC_checkout_1;
create table visits_funnel_addtoSC_checkout_1 as
select distinct d.device
, pr.da
,sc.*
from sc_temp3 as sc
left join etl.pixel_raw2 as pr 
on sc.uid=pr.uid
and pr.datetime_cst::date BETWEEN first_addto_SC::date
and (case when sc.first_order is not null then sc.last_order::date else sc.last_addto_SC::date end)
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id;

drop table if exists device_switch_one_sp;
create table device_switch_one_sp as
select distinct 
first_value(d.device) over (partition by sc.uid,sc.LPD_week_start  order by pr.datetime_cst ) as start_device
, first_value(d.device) over (partition by sc.uid,sc.LPD_week_start  order by pr.datetime_cst desc ) as last_device
,sc.uid
,sc.LPD_week_start
,sc.style_count
, sc.first_order
,sc.first_addto_SC
from sc_temp3 as sc
left join etl.pixel_raw2 as pr 
on sc.uid=pr.uid
and pr.datetime_cst::date BETWEEN first_addto_SC::date
and (case when sc.first_order is not null then sc.last_order::date else sc.last_addto_SC::date end)
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id
where sc.first_addto_SC between '2016-07-01' and '2016-07-04' and d.device <> 'Suspected Bots';

select * from device_switch_one_sp as sc order by sc.uid,sc.LPD_week_start;

select * from visits_funnel_addtoSC_checkout where uid = 1001621 and LPD_week_start = '2016-07-31';
select d.device,pr.datetime_cst from etl.pixel_raw2 as pr 
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id
where uid = 1001621 and datetime_cst::date BETWEEN '2016-07-01'and '2016-10-01'
order by pr.datetime_cst;

--- Distribution of devices for checkout & addtocart
drop table if exists visits_funnel_addtoSC_checkout_3;
create table visits_funnel_addtoSC_checkout_3 as
select distinct d.device
,pr.action_type
,pr.datetime_cst
,sc.*
from sc_temp3 as sc
left join etl.pixel_raw2 as pr 
on sc.uid=pr.uid
and pr.datetime_cst::date BETWEEN first_addto_SC::date
and (case when sc.first_order is not null then sc.last_order::date else sc.last_addto_SC::date end)
inner join rtrbi.browser_device as bd on pr.browser_id = bd.browser_id
inner join rtrbi.devices as d on d.device_id=bd.device_id
where  action_type = 'rezo_add_to_cart' or action_type = 'checkout_complete';


select vf.device
, vf.action_type 
, datetime_cst 
,vf.uid
,vf.LPD_week_start
, a.device_count
,style_count
, first_order
,first_addto_SC
from
visits_funnel_addtoSC_checkout_3 as vf
left join 
(select uid 
,LPD_week_start
, count(distinct device) as device_count
from visits_funnel_addtoSC_checkout_3 
group by uid,LPD_week_start) as a
on vf.uid = a.uid and vf.LPD_week_start = a.LPD_week_start

select distinct device from rtrbi.devices;
select count(*) from sc_temp3;