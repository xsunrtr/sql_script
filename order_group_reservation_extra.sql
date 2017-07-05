-------------------------------------------------------------------------------------------------------------------
/*Add fulfillment status, inventory_unit location of extras to existing order_group_reservation table*/

-----------1. get fulfillment_last_status info for only extras---------------------------------
drop table if exists rtrbi.fulfillment_last_status_extra;
create table rtrbi.fulfillment_last_status_extra as
select
    f.booking_id
,    f.id as fulfillment_id
,    rb.sku
,    f.child_sku
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
    and f.child_sku is not null
inner join etl.reservation_booking as rb
    on f.booking_id=rb.id;


 -----------2. create lookup table for both dresses and extras --------------------------------- 
drop table if exists analytics.inventory_unit_extra_id_barcode_lookup_temp;
create table analytics.inventory_unit_extra_id_barcode_lookup_temp as
select distinct iu.id
,     iu.barcode
from etl.inventory_unit as iu;

 -----------3. get inventory_unit_last_location info for both dresses and extras -----------------
drop table if exists analytics.inventory_unit_extra_last_location_temp;
drop table if exists analytics.inventory_unit_extra_last_location_temp;
create table analytics.inventory_unit_extra_last_location_temp as
select iu.id as unit_id
,     iu.sku as child_sku
,     iu.barcode
,     iu.status
,     iu.action
,     iu.location_station
,     iu.updated_on
from etl.inventory_unit as iu
inner join etl.inventory_unit_seq as ius
on iu.id = ius.unit_id and
   iu.seq = ius.seq;

 -----------4. join info of extras with existing ogr table-----------------
drop table if exists analytics.order_group_reservation_withExtras;
create table  analytics.order_group_reservation_withExtras as
select ogr.uid
,ogr.order_id
,ogr.order_datetime_est
,ogr.billing_postal_code
,ogr.order_status
,ogr.order_type
,ogr.mail
,ogr.user_create_date
,ogr.group_id
,ogr.delivery_postal_code
,ogr.ups_ground_shipping_zone
,ogr.reservation_booking_id
,ogr.reservation_created
,ogr.expected_begin_date
,ogr.expected_end_date
,ogr.actual_expected_end_date
,ogr.earliest_possible_begin_date
,ogr.last_possible_begin_date
,ogr.sku
,flse.child_sku
,ogr.merchandise_category
,ogr.primary_booking_id
,ogr.rental_begin_date
,ogr.rental_end_date
,ogr.customer_shipment_date
,ogr.warehouse_received_date
,ogr.item_available_date
,ogr."style"
,ogr.product_type
,ogr.barcode as order_barcode
,flse.barcode
,flse.fulfillment_id
,flse.last_fulfillment_status
,flse.last_fulfillment_updated_on
,flse.last_fulfillment_seq
,flse.last2nd_fulfillment_status
,flse.last2nd_fulfillment_updated_on
,iull.unit_id
,count(
  case
    when p.type = 'D' and flse.child_sku is not null
    --and flse.last_fulfillment_status != 'ITEM_LIMBO'
      then ogr.reservation_booking_id
    else null end
  ) over (partition by ogr.group_id) as extras_in_group
,iull.status as unit_current_status
,iull.action as unit_last_action
,iull.location_station as unit_current_location
,iull.updated_on as unit_last_updated_on
,ogr.rf_end_date
,ogr.unit_usage_rank
,ogr.unit_total_usage
from rtrbi.order_group_reservation as ogr
inner join rtrbi.fulfillment_last_status_extra as flse 
on ogr.reservation_booking_id=flse.booking_id and flse.fulfillment_seq = flse.last_fulfillment_seq
inner join analytics.products_iid as pi on lower(flse.sku) = lower(pi.sku)
inner join analytics.products as p on pi.style = p.style
left join analytics.inventory_unit_extra_id_barcode_lookup_temp as iul on flse.barcode = iul.barcode
left join analytics.inventory_unit_extra_last_location_temp as iull on iul.id = iull.unit_id;

            
drop table rtrbi.fulfillment_last_status_extra;
drop table analytics.inventory_unit_extra_last_location_temp;
drop table analytics.inventory_unit_extra_id_barcode_lookup_temp;
