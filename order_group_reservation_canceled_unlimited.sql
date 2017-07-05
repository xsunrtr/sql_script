drop table if exists rtrbi.fulfillment_unlimited_last_status_temp;
create table rtrbi.fulfillment_unlimited_last_status_temp as
select
    f.booking_id
,    f.id as fulfillment_id
,    f.status as last_fulfillment_status
,    f.barcode
,    cast(f.updated_on at time zone 'US/EASTERN' as timestamptz) as last_fulfillment_updated_on
,    f.seq as fulfillment_seq, fs.seq as last_fulfillment_seq
,    lag(f.status) over (partition by f.booking_id order by f.updated_on) as last2nd_fulfillment_status
,    lag(cast(f.updated_on at time zone 'US/EASTERN' as timestamptz)) over (partition by f.booking_id order by f.updated_on) as last2nd_fulfillment_updated_on
from etl.fulfillment as f
inner join etl.fulfillment_seq as fs
    on f.id = fs.fulfillment_id and
    f.seq between fs.seq - 1 and fs.seq
where f.child_sku is null;


drop table if exists etl.ups_ground_shipping_zone_unlimited_07094_temp;
create table etl.ups_ground_shipping_zone_unlimited_07094_temp as
with all_zipcode as (
  select distinct smt.destination_zip_code
  from etl.shipping_method_time smt)
select az.destination_zip_code
,     coalesce(gnd.transit_days, car.transit_days, 5) as transit_days
from all_zipcode as az
left join etl.shipping_method_time as car
on car.destination_zip_code = az.destination_zip_code and
   car.shipping_method_id = 6
left join etl.shipping_method_time as gnd
on gnd.destination_zip_code = az.destination_zip_code and
   gnd.shipping_method_id = 2;


drop table if exists rtrbi.barcode_unit_id_match_unlimited_temp;
create table rtrbi.barcode_unit_id_match_unlimited_temp as
select ih.barcode, max(ih.unit_id) as id
from rtrbi.inventory_history as ih
group by ih.barcode;


drop table if exists analytics.inventory_unit_id_barcode_lookup_unlimited_temp;
create table analytics.inventory_unit_id_barcode_lookup_unlimited_temp as
select distinct iu.id
,     iu.barcode
from etl.inventory_unit as iu;


drop table if exists analytics.inventory_unit_last_location_unlimited_temp;
create table analytics.inventory_unit_last_location_unlimited_temp as
select iu.id
,     iu.barcode
,     iu.status
,     iu.action
,     iu.location_station
,     iu.updated_on
from etl.inventory_unit as iu
inner join etl.inventory_unit_seq as ius
on iu.id = ius.unit_id and
   iu.seq = ius.seq;


-----------------------------------------------------------------

drop table if exists rtrbi.order_group_reservation_canceled_unlimited_temp;
create table rtrbi.order_group_reservation_canceled_unlimited_temp as
select
distinct uco.uid
,uco.order_id
,cast(to_timestamptz(uco.created) at time zone 'US/EASTERN' as timestamptz) as order_datetime_est
,uco.billing_postal_code
,uco.order_status
,uco.order_type
,u.mail
,u.create_date as user_create_date
,rog.group_id
,rog.postal_code as delivery_postal_code
,cast(rog.created_at at time zone 'US/EASTERN' as timestamptz) as group_created_est
--  ,sz.zone_code as ups_ground_zone_code
,gsz.transit_days as ups_ground_shipping_zone
,rb.id as reservation_booking_id
,rb.creation_date as reservation_created
,rb.expected_begin_date
,rb.expected_end_date
,case
  when gsz.transit_days = 1
    and datediff('day',rbd.rental_begin_date,rbd.rental_end_date) = 3
    and dayofweek(rb.expected_begin_date) != 2
    then date(rb.expected_end_date - interval '1 day')
  when gsz.transit_days >= 2
    and datediff('day',rbd.rental_begin_date,rbd.rental_end_date) = 3
    and dayofweek(rb.expected_begin_date) = 7
    then date(rb.expected_end_date + interval '2 day')
  when gsz.transit_days >= 2
    and datediff('day',rbd.rental_begin_date,rbd.rental_end_date) = 3
    and dayofweek(rb.expected_begin_date) = 2
    then date(rb.expected_end_date + interval '1 day')
  else rb.expected_end_date end as actual_expected_end_date
,rb.earliest_possible_begin_date
,rb.last_possible_begin_date
,rb.sku
,rb.merchandise_category
,rb.primary_booking_id
,date(cast(rbd.rental_begin_date at time zone 'US/EASTERN' as timestamptz)) as rental_begin_date
,date(cast(rbd.rental_end_date at time zone 'US/EASTERN' as timestamptz)) as rental_end_date
,date(cast(rbd.customer_shipment_date at time zone 'US/EASTERN' as timestamptz)) as customer_shipment_date
,date(cast(rbd.warehouse_received_date at time zone 'US/EASTERN' as timestamptz)) as warehouse_received_date
,date(cast(rbd.item_available_date at time zone 'US/EASTERN' as timestamptz)) as item_available_date
,pi.style
,p.type as product_type
,f.barcode
,f.fulfillment_id
,f.last_fulfillment_status
,f.last_fulfillment_updated_on
,f.last2nd_fulfillment_status
,f.last2nd_fulfillment_updated_on
,case
  when qrf.new_rf_end_date is not null then qrf.new_rf_end_date
  else date(rf.end_date) end as rf_end_date
,ut.id as unit_id
,count(
  case
    when p.type = 'D'
      and f.last_fulfillment_status != 'ITEM_LIMBO'
      then rb.id
    else null end
  ) over (partition by rog.group_id) as dresses_in_group
,iull.status as unit_current_status
,iull.action as unit_last_action
,iull.location_station as unit_current_location
,iull.updated_on as unit_last_updated_on
from etl.uc_orders as uco
inner join analytics.users as u
    on uco.uid = u.uid
inner join etl.rtr_order_groups as rog
    on uco.order_id = rog.order_id
inner join etl.reservation_booking as rb
    on rog.group_id = rb.order_group_id
inner join etl.reservation_booking_detail as rbd
    on rb.id = rbd.reservation_booking_id
inner join analytics.products_iid as pi
    on lower(rb.sku) = lower(pi.sku)
inner join analytics.products as p
    on pi.style = p.style
inner join rtrbi.fulfillment_unlimited_last_status_temp as f
    on rb.id = f.booking_id and
       f.fulfillment_seq = f.last_fulfillment_seq
left join rtrbi.barcode_unit_id_match_unlimited_temp as ut
on f.barcode = ut.barcode
left join etl.reservation_fulfillment as rf
    on rb.id = rf.reservation_booking_id
left join etl.ups_ground_shipping_zone_unlimited_07094_temp as gsz
on rog.postal_code = gsz.destination_zip_code
left join rtrbi.queue_rf_end_date_consolidation as qrf
on rb.id = qrf.reservation_booking_id
left join analytics.inventory_unit_id_barcode_lookup_unlimited_temp as iul
on f.barcode = iul.barcode
left join analytics.inventory_unit_last_location_unlimited_temp as iull
on iul.id = iull.id
where rb.status=1 and rb.type = 0 -- cancellation filter-----
    and uco.order_status in ('canceled', 'payment_received')
    and uco.order_type = 'QUEUE';
    
    
drop table if exists rtrbi.order_group_reservation_canceled_unlimited;
create table rtrbi.order_group_reservation_canceled_unlimited as
select gr.*,
     case when gr.unit_id is not null and length(gr.barcode) = 8
      then count(gr.reservation_booking_id) over (partition by gr.unit_id order by gr.rental_begin_date) else null end as unit_usage_rank,
     case when gr.unit_id is not null and length(gr.barcode) = 8
      then count(gr.reservation_booking_id) over (partition by gr.unit_id) else null end as unit_total_usage
from rtrbi.order_group_reservation_canceled_unlimited_temp as gr;


drop table rtrbi.fulfillment_unlimited_last_status_temp;
drop table analytics.inventory_unit_last_location_unlimited_temp;
drop table etl.ups_ground_shipping_zone_unlimited_07094_temp;
drop table analytics.inventory_unit_id_barcode_lookup_unlimited_temp;
drop table rtrbi.barcode_unit_id_match_unlimited_temp;
