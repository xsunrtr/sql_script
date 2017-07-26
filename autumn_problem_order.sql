/* Autumn - Order Issues/ Problem Orders */



----1. View for Autumn - Order Issues/Problem Orders/Classic PO/Classic PO1(classic groups/bookings PO summary table) -------------------------------
----2. View for Autumn - Order Issues/Problem Orders/Classic PO/Classic PO detail(classic bookings PO detail table) --------------------------------
select distinct apr.group_id
, apr.reservation_booking_id
, case when ogre.child_sku is not null then apr.reservation_booking_id else null end as extra_reservation_booking_id
, apr.rental_begin_date
, apr.merchandise_category
, apr."type"
, apr.sub_type
, case when apr.po_category is not null then apr.po_category
    when apr.po_category is null and ogre.last_fulfillment_status = 'ITEM_LIMBO' then 'Extra PO'
    end as po_category
, CASE WHEN apr.po_category = 'Predicted' and lower(apr.po_reason) like '%late%' then 'Lateness'
     	 WHEN lower(apr.po_reason) like '%allogator%' then 'ALLOGATOR UNKNOWN'
     	 WHEN apr.po_category is null and apr.po_reason = 'Predicted' then NULL
     	 WHEN apr.po_category = 'Warehouse' and lower(apr.po_reason) like '%miss%' then 'Missing'
        ELSE apr.po_reason END as po_reason 
, case when apr.po_category is not null then apr.group_id else null end as PO_parent_group_id
, case when apr.po_category is not null or ogre.last_fulfillment_status = 'ITEM_LIMBO'
    then apr.group_id else null end as PO_parent_extra_group_id
, case when apr.po_category is not null 
	then apr.reservation_booking_id else null end as PO_parent_Reservations_booking_id
, case when ogre.last_fulfillment_status = 'ITEM_LIMBO' or apr.po_category is not null 
	then apr.reservation_booking_id else null end as Parent_Extras_PO_Reservations_booking_id
from rtrbi.autumn_report_raw_data as apr
inner join etl.products_master_iid as pmi on apr.sku = pmi.sku
and apr.order_type in ('CLASSIC','CLASSIC_POSH','STORE_PICKUP','STORE_PICKUP_WEB')
and apr.rental_begin_date > current_date - interval '8 months'
and pmi.special_type <>'X'
left join analytics.order_group_reservation_withExtras as ogre
on ogre.reservation_booking_id = apr.reservation_booking_id
;




----3. View for Autumn - Order Issues/Problem Orders/Unlimited PO/Unlimited PO1(unlimited groups/bookings PO summary table) -------------------------------
----4. View for Autumn - Order Issues/Problem Orders/Unlimited PO detail/Unlimited PO1(unlimited bookings PO detail table)----------------------
select distinct 
case when po_category is not null or ogrw.cancel_flag=0 then ogrw.group_id else null end as group_id 
, case when po_category is not null or ogrw.cancel_flag=0 then ogrw.reservation_booking_id else null end as reservation_booking_id
, case when ogrw.cancel_flag=0 then ogrw.group_id else null end as not_canceled_group_id 
, ogrw.rental_begin_date
, ogrw.merchandise_category
, ogrw.product_type as type
, pmi.sub_type
, ups.po_category 
, ogrw.child_sku
,ups.po_reason
, case when ogrw.child_sku is null then 'ParentSku' else 'extra' end as ParentSku_extra
, case when ups.po_category is null and ogrw.child_sku is not null 
then ogrw.group_id else null end as PO_parent_group_id
, case when ups.po_category is not null then ogrw.group_id else null end as PO_parent_extra_group_id
, case when ups.po_category is not null and ogrw.child_sku is null
	then ogrw.reservation_booking_id else null end as PO_parent_Reservation_booking_id
, case when ups.po_category is not null
	then ogrw.reservation_booking_id else null end as Parent_Extras_PO_Reservation_booking_id
 from 
  ( select  fulfillment_id
 			,rental_begin_date
 			,merchandise_category
			,product_type
			,order_id
			,group_id
			,reservation_booking_id
			,sku
			,child_sku
			,last_fulfillment_status
			,last2nd_fulfillment_status
			,last_fulfillment_updated_on
			,last2nd_fulfillment_updated_on
			, 1 as cancel_flag
    from    rtrbi.order_group_reservation_canceled_unlimited as ogrq
    where ogrq.rental_begin_date > current_date - interval '24 months'
    union
	select  ogr.fulfillment_id
 			,ogr.rental_begin_date
			,ogr.merchandise_category
			,ogr.product_type
			,ogr.order_id
			,ogr.group_id
			,ogr.reservation_booking_id
			,ogr.sku
			,ogre.child_sku as child_sku
			,ogre.last_fulfillment_status
			,ogre.last2nd_fulfillment_status
			,ogre.last_fulfillment_updated_on
			,ogre.last2nd_fulfillment_updated_on
			, 0 as cancel_flag
    from    rtrbi.order_group_reservation as ogr
    left join analytics.order_group_reservation_withExtras as ogre
    on ogre.reservation_booking_id = ogr.reservation_booking_id
    where ogr.order_type = 'QUEUE' and ogr.rental_begin_date > current_date - interval '24 months'
) as ogrw
left join rtrbi.unlimited_po_sources as ups 
 on ogrw.fulfillment_id = ups.fulfillment_id
inner join etl.products_master_iid as pmi on ogrw.sku = pmi.sku
where ogrw.rental_begin_date > current_date - interval '8 months'
;



