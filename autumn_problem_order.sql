/* Autumn - Order Issues/ Problem Orders */



----1. View for Autumn - Order Issues/Problem Orders/Classic & Unlimited -------------------------------

create or replace view rpt.classic_po_table as
(select distinct 'Classic' as order_type
, apr.group_id
, apr.reservation_booking_id
, case when ogre.child_sku is not null then apr.reservation_booking_id else null end as extra_reservation_booking_id
, apr.rental_begin_date
, apr.merchandise_category
, apr."type"
, case when apr.mail like '%renttherunway.com%' then 'rtr_email'
				else 'non rtr_email' end as email
, apr.sub_type
, CASE when apr.po_category is null and ogre.last_fulfillment_status = 'ITEM_LIMBO' then 'Warehouse'
  else apr.po_category end as po_category
, CASE when apr.po_category is null and ogre.last_fulfillment_status = 'ITEM_LIMBO' then 'Extra PO'
        WHEN apr.po_category = 'Predicted' and lower(apr.po_reason) like '%late%' then 'Lateness'
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
on ogre.reservation_booking_id = apr.reservation_booking_id)
union 
(
    select distinct 'Unlimited' as order_type
, case when ogrw.last_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
    or ogrw.last2nd_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS') 
    or ogrw.cancel_flag=0 then ogrw.group_id else null end as group_id 
, case when ogrw.last_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
    or ogrw.last2nd_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
    or ogrw.cancel_flag=0 then ogrw.reservation_booking_id else null end as reservation_booking_id
, case when ogrw.child_sku is not null then ogrw.reservation_booking_id  else null end as extra_reservation_booking_id
, ogrw.rental_begin_date
, ogrw.merchandise_category
, ogrw.product_type as type
, case when ogrw.mail like '%renttherunway.com%' then 'rtr_email'
                else 'non rtr_email' end as email
, pmi.sub_type
, CASE when ups.po_category is null 
  and (ogrw.last_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
  or ogrw.last2nd_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS'))
  then 'Warehouse' else ups.po_category  end as po_category
, CASE when ups.po_category is null
  and (ogrw.last_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM','PROBLEM_IN_PROGRESS' )
  or ogrw.last2nd_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM','PROBLEM_IN_PROGRESS' ))
  then 'Extra PO' else ups.po_reason end as po_reason
, case when ups.po_category is not null 
then ogrw.group_id else null end as PO_parent_group_id
, case when ups.po_category is not null 
or ogrw.last_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
or ogrw.last2nd_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
then ogrw.group_id else null end as PO_parent_extra_group_id
, case when ups.po_category is not null 
    then ogrw.reservation_booking_id else null end as PO_parent_Reservation_booking_id
, case when ups.po_category is not null 
    or ogrw.last_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM','PROBLEM_IN_PROGRESS' )
    or ogrw.last2nd_fulfillment_status in ('ITEM_LIMBO', 'PROBLEM_ITEM' ,'PROBLEM_IN_PROGRESS')
    then ogrw.reservation_booking_id else null end as Parent_Extras_PO_Reservation_booking_id
 from 
  ( --- canceled unlimited (parent + child skus)
  select  fulfillment_id
            ,rental_begin_date
            ,merchandise_category
            ,product_type
            ,order_id
            ,group_id
            ,reservation_booking_id
            ,sku
            ,child_sku
            ,ogrq.mail
            ,last_fulfillment_status
            ,last2nd_fulfillment_status
            ,last_fulfillment_updated_on
            ,last2nd_fulfillment_updated_on
            , 1 as cancel_flag
    from    rtrbi.order_group_reservation_canceled_unlimited as ogrq
    where ogrq.rental_begin_date > current_date - interval '24 months'
    union
    --- non_canceled unlimited (parent skus)
    select  ogr.fulfillment_id
            ,ogr.rental_begin_date
            ,ogr.merchandise_category
            ,ogr.product_type
            ,ogr.order_id
            ,ogr.group_id
            ,ogr.reservation_booking_id
            ,ogr.sku
            ,null as child_sku
            ,ogr.mail
            ,ogr.last_fulfillment_status
            ,ogr.last2nd_fulfillment_status
            ,ogr.last_fulfillment_updated_on
            ,ogr.last2nd_fulfillment_updated_on
            , 0 as cancel_flag
    from  rtrbi.order_group_reservation as ogr
    where ogr.order_type = 'QUEUE' and ogr.rental_begin_date > current_date - interval '24 months'
    union
    --- non_canceled unlimited (extra skus)
    select  ogre.fulfillment_id
            ,ogre.rental_begin_date
            ,ogre.merchandise_category
            ,ogre.product_type
            ,ogre.order_id
            ,ogre.group_id
            ,ogre.reservation_booking_id
            ,ogre.sku
            ,ogre.mail
            ,ogre.child_sku
            ,ogre.last_fulfillment_status
            ,ogre.last2nd_fulfillment_status
            ,ogre.last_fulfillment_updated_on
            ,ogre.last2nd_fulfillment_updated_on
            , 0 as cancel_flag
    from analytics.order_group_reservation_withExtras as ogre
    where ogre.order_type = 'QUEUE' and ogre.rental_begin_date > current_date - interval '24 months'
) as ogrw
left join rtrbi.unlimited_po_sources as ups 
 on ogrw.fulfillment_id = ups.fulfillment_id
inner join etl.products_master_iid as pmi on ogrw.sku = pmi.sku
where ogrw.rental_begin_date > current_date - interval '8 months'
)





