/*  Illinois Tax Monthly Report
 * nth_entry: if the unit was activated before 2018-01-01 then -1,
 * 			  if the unit was activated after 2018-01-01 then it will be the nth time it entered IL.
 * */
select distinct
d.calendar_year
, d.calendar_month
, og.state
, og.city
, ogr.barcode
, ogr.sku
, fvi.cost as unit_cost
, og.shipping_cost
, fvi.activated_on
, s.ship_date
, og.postal_code
, case when fvi.activated_on::date < '2018-01-01' then -1
else row_number() over (partition by ogr.barcode order by s.ship_date) end as nth_entry
from etl.rtr_order_groups og
INNER JOIN etl.uc_zones z 
ON z.zone_id = og.state
INNER JOIN rtrbi.order_group_reservation ogr 
on ogr.group_id = og.group_id
INNER JOIN analytics.finance_current_view_inventory as fvi
on ogr.barcode = fvi.barcode
INNER JOIN etl.shipment as s 
on og.group_id = s.shipper_reference
INNER JOIN rtrbi.dates d 
on s.ship_date::date = d.asofdate
where zone_code = 'IL'
order by barcode, nth_entry;
