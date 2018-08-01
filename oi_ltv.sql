USE RTR_PROD;
show columns in table rtrbi.profitability_sku_monthly_revenue_cogs;

USE RTR_STAGE;
--SELECT * FROM etl.yp_autumn_full;
SELECT DISTINCT contact_method
FROM etl.yp_autumn_full af
WHERE af.annotation_group IN ('SHIPPING','RFF','QUALITY_ISSUE')
--AND af.order_type = 'QUEUE'
AND  af.detail_name in ('reason','fitOverall');
AND contact_method <> 'AUTOMATED';

SELECT max(transaction_created)
,min(transaction_created)
, max(po.order_datetime_est)
FROM etl.yp_autumn_full af 
INNER JOIN analytics.ogr_po po 
ON af.order_id = po.order_id
WHERE --af.annotation_group IN ('SHIPPING','RFF','QUALITY_ISSUE')
af.annotation_group ='RFF'
AND  
af.detail_name in ('reason','fitOverall')
AND order_type = 'CLASSIC'
AND af.annotation_group = 'RFF'
AND af.annotation_target = 'BOOKING'
AND  po.merchandise_category = 'RENTAL'
AND regexp_like(po.sku, '^SS\d+') = FALSE
AND regexp_like(po.sku, '^XX\d+') = FALSE;

SELECT max(order_datetime_est)
FROM analytics.ogr_po AS po 
INNER JOIN etl.yp_autumn_full as af
ON af.annotation_id = po.reservation_booking_id
--AND af.annotation_group ='RFF'
WHERE order_type = 'CLASSIC'
AND af.annotation_group = 'RFF'
AND af.annotation_target = 'BOOKING'
AND  po.merchandise_category = 'RENTAL'
AND regexp_like(po.sku, '^SS\d+') = FALSE
AND regexp_like(po.sku, '^XX\d+') = FALSE;



SELECT DISTINCT annotation_group, detail_value
FROM etl.yp_autumn_full as af 
                    where af.annotation_group = 'REPLACEMENT'
                      and af.detail_name = 'reason'
ORDER BY 1 ;

SELECT count(DISTINCT order_id ) AS order_cnt
FROM etl.yp_autumn_full af 
WHERE af.annotation_group ='SHIPPING'
AND af.annotation_target = 'BOOKING'
--IN ('SHIPPING','RFF','QUALITY_ISSUE')
AND  af.detail_name in ('reason','fitOverall');

SELECT * FROM analytics.ogr_po  LIMIT 200;
USE RTR_prod;
  
/*Step 0*/
 create or replace view rpt.v_refund_order as
 select step0.order_id
 ,step0.fli_step_zero_amount
 , total.fli_amount
 , step0.fli_step_zero_amount-total.fli_amount as refund
from ( select fli.order_id
             , sum(case when fli.direction='OUT' then 0-fli.subtotal else fli.subtotal end) as fli_step_zero_amount
        from etl.financial_line_item as fli
        inner join etl.fli_status_history as fsh
        on fli.id = fsh.fli_id
        inner join etl.invoice_order as io
        on io.id = fli.invoice_id
        inner join etl.uc_orders as uco
        on uco.order_id = fli.order_id /*and
           to_timestamptz(uco.created) between (fli.created_at at time zone 'US/CENTRAL' at time zone 'UTC') - interval '100 seconds'
                              and (fli.created_at at time zone 'US/CENTRAL' at time zone 'UTC')  + interval '100 seconds'
        */where fsh.to_status = 'CONFIRMED'
        --  and uco.created::date > '2015-10-01'
          and io.seq = 0
        group by 1) step0
  inner join     (
  select fli.order_id
	 , uco.order_type
	 , dt.fiscal_week_start
         , dt.fiscal_month_start
	 , rog.rental_begin_date
	 , sum(case when fli.direction='OUT' then 0-fli.subtotal else fli.subtotal end) as fli_amount
from etl.financial_line_item as fli
inner join etl.fli_status_history as fsh
on fli.id = fsh.fli_id
inner join rtrbi.order_group_reservation as rog
on fli.group_id = rog.group_id
inner join rtrbi.dates as dt
on rog.rental_begin_date = dt.asofdate
inner join etl.uc_orders as uco
on uco.order_id = fli.order_id
where fsh.to_status = 'CONFIRMED'
  and dt.fiscal_year >= 2015
  and uco.primary_email not like '%renttherunway.com'
group by 1,2,3,4,5) as total 
on total.order_id = step0.order_id;

SELECT * FROM rpt.v_refund_order;

/*
 * Step1 
 * order issue booking level data*/
DROP view IF EXISTS rpt.ogr_order_issue;
CREATE view rpt.ogr_order_issue AS 
WITH replacement as
(
	SELECT  DISTINCT af.order_id
	, ogr.group_id
	, af.annotation_target_id
	, af.detail_value
	FROM etl.yp_autumn_full as af
	INNER JOIN rtrbi.order_group_reservation ogr
	ON af.annotation_target_id = ogr.reservation_booking_id
	where af.annotation_group = 'REPLACEMENT'
	and af.detail_name = 'reason' )
,refund AS (
select fli.order_id
, sum(fli.subtotal) as refund_amount
from etl.financial_line_item as fli
inner join etl.fli_status_history as fsh
on fli.id = fsh.fli_id
and fsh.to_status = 'CONFIRMED'
  --and type = 'MERCHANDISE_CREDIT'
  and direction = 'OUT'
  group by 1
)
SELECT DISTINCT
 po.order_id
 ,po.uid
, po.group_id
, po.order_datetime_est
, po.group_created_est
, po.reservation_booking_id
, po.reservation_created
, po.email
, po.TYPE
, po.last_possible_begin_date
, po.rental_begin_date
, rp.order_id AS order_id_w_rplm
, rp.detail_value
, COALESCE(rf.refund_amount,0) AS refund_amount
, COALESCE(vrf.refund,0) as fli_credit_refund
, CASE WHEN af.annotation_group = 'RFF' THEN po.order_id END AS fit_order_id
, CASE WHEN af.annotation_group = 'RFF' AND rp.detail_value = 'FIT' THEN po.order_id END AS rpl_fit_order_id
, CASE WHEN af.annotation_group = 'QUALITY_ISSUE' THEN po.order_id END AS quality_order_id
, CASE WHEN af.annotation_group = 'QUALITY_ISSUE' AND rp.detail_value like '%QUALITY%' THEN po.order_id END AS rpl_quality_order_id
, CASE WHEN af.annotation_group = 'SHIPPING' THEN po.order_id END AS delivery_order_id
, CASE WHEN af.annotation_group = 'SHIPPING' AND rp.detail_value in ('CARRIER_ISSUE','OPERATION_ISSUE') --CARRIER_ISSUE OR OPERATION_ISSUE
THEN po.order_id END AS rpl_delivery_order_id
, CASE WHEN po_category IS NOT NULL  THEN po.order_id END AS po_order_id
, CASE WHEN po_category IS NOT NULL  THEN po.group_id END AS po_group_id
, CASE WHEN po_category IS NOT NULL AND rp.detail_value LIKE '%PO%'   THEN rp.order_id END AS rpl_po_order_id
, CASE WHEN po_category IS NOT NULL AND rp.detail_value LIKE '%PO%'   THEN rp.group_id END AS rpl_po_group_id
FROM analytics.ogr_po AS po 
LEFT JOIN etl.yp_autumn_full as af
ON po.order_id = af.order_id
LEFT JOIN replacement rp 
ON rp.order_id = po.order_id
LEFT JOIN refund rf
ON rf.order_id = po.order_id
left JOIN rpt.v_refund_order vrf
ON po.order_id = vrf.order_id
WHERE order_type = 'CLASSIC'
AND  po.merchandise_category = 'RENTAL'
AND regexp_like(po.sku, '^SS\d+') = FALSE
AND regexp_like(po.sku, '^XX\d+') = FALSE;


CREATE OR REPLACE VIEW rpt.v_ogr_order_issue_order_level as
SELECT ooi.order_id
, ooi.uid
, ooi.rental_begin_date
, ooi.email
, ooi.detail_value
,  COALESCE(ooi.refund_amount ,0) AS refund_amount
--, COALESCE(ooi.fli_credit_refund,0) AS fli_credit_refund
, count(DISTINCT ooi.group_id) AS group_cnt
, max(ooi.order_id_w_rplm) AS rpl_order_id
, max(ooi.fit_order_id) AS fit_order_id
, max(ooi.quality_order_id) AS quality_order_id
, max(ooi.delivery_order_id) AS delivery_order_id
, max(ooi.po_order_id) AS po_order_id
, max(ooi.rpl_fit_order_id) AS rpl_fit_order_id
, max(ooi.rpl_quality_order_id) AS rpl_quality_order_id
, max(ooi.rpl_delivery_order_id) AS rpl_delivery_order_id
, max(CASE WHEN rpl_po_group_id <> po_group_id THEN ooi.po_order_id end) AS rpl_po_order_id
FROM rpt.ogr_order_issue ooi
GROUP BY 1,2,3,4,5,6;

grant all on view 
rpt.v_ogr_order_issue_order_level to tableau_role;

/*Customer LTV*/
SELECT * FROM  rpt.v_order_issue_metrics_summary LIMIT 200;

create or replace view rpt.v_order_issue_metrics_summary as
select distinct gd.*
, D.asofdate
, d.fiscal_week_start
, d.fiscal_month_start
, d.fiscal_year
, oo.nth_order
, COALESCE(gd.refund_amount,0) as fli_credit_refund
, case WHEN po_order_id is not null then 'PO'
when delivery_order_id is not null then 'Delivery'
/*Make sure it's the original PO group instead of resolution group*/
when quality_order_id is not null then 'Quality'
WHEN fit_order_id is not null THEN 'Fit'
else 'non_PO' end as order_issue_category
from rpt.v_ogr_order_issue_order_level as gd
inner join rtrbi.dates as d 
on gd.rental_begin_date = d.asofdate
left join rtrbi.v_ot_orderdata as oo
on gd.order_id = oo.order_id;

 /* Repeat Rate*/ 

CREATE OR REPLACE VIEW rpt.v_order_issue_loyalty_metrics as
 select 
 cms.uid
 , cms.email
 , cms.rental_begin_date
 , cms.order_issue_category
 , CASE WHEN (PO_ORDER_ID IS NOT NULL AND RPL_PO_ORDER_ID IS NOT NULL )
OR (DELIVERY_ORDER_ID IS NOT NULL AND RPL_DELIVERY_ORDER_ID IS NOT null)
OR (QUALITY_ORDER_ID IS NOT NULL AND RPL_QUALITY_ORDER_ID IS NOT null)
or(FIT_ORDER_ID IS NOT NULL AND RPL_FIT_ORDER_ID IS NOT null) THEN 'W. Replacement'
ELSE 'No Replacement' END AS replacement_flg
 , cms.nth_order
 , CASE WHEN refund_amount = 0 THEN 'no_refund' ELSE 'refund' END refund_flg
 , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 30, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_30d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 60, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_60d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 90, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_90d
   , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 180, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_180d
    , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 270, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_270d
     , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 365, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_365d
 from rpt.v_order_issue_metrics_summary as cms
 left join rtrbi.order_group_reservation as ogr
 on cms.uid = ogr.uid
 group by 1,2,3,4,5,6,7;
 
 ---- PO -----
 CREATE OR REPLACE VIEW rpt.v_loyalty_metrics_po as
 select 
 cms.uid
 , cms.email
 , cms.rental_begin_date
 , CASE WHEN PO_ORDER_ID IS NOT NULL THEN 'PO' ELSE 'NON_PO' END AS order_issue
 , CASE WHEN (PO_ORDER_ID IS NOT NULL AND RPL_PO_ORDER_ID IS NOT NULL ) THEN 'W. Replacement'
ELSE 'No Replacement' END AS replacement_flg
 , cms.nth_order
 , CASE WHEN refund_amount = 0 THEN 'no_refund' ELSE 'refund' END refund_flg
 , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 30, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_30d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 60, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_60d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 90, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_90d
   , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 180, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_180d
     , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 270, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_270d
     , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 365, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_365d
 from rpt.v_order_issue_metrics_summary as cms
 left join rtrbi.order_group_reservation as ogr
 on cms.uid = ogr.uid
 group by 1,2,3,4,5,6,7;
 
 GRANT ALL ON VIEW rpt.v_loyalty_metrics_po TO tableau_role;
 SELECT * FROM rpt.v_loyalty_metrics_po;
 
 CREATE OR REPLACE VIEW rpt.v_loyalty_metrics_quality as
 select 
 cms.uid
 , cms.email
 , cms.rental_begin_date
 , CASE WHEN QUALITY_ORDER_ID IS NOT NULL THEN 'QUALITY' ELSE 'NON_QUALITY' END AS order_issue
 , CASE WHEN (QUALITY_ORDER_ID IS NOT NULL AND RPL_QUALITY_ORDER_ID IS NOT NULL ) THEN 'W. Replacement'
ELSE 'No Replacement' END AS replacement_flg
 , cms.nth_order
 , CASE WHEN refund_amount = 0 THEN 'no_refund' ELSE 'refund' END refund_flg
 , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 30, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_30d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 60, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_60d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 90, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_90d
   , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 180, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_180d
      , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 270, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_270d
     , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 365, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_365d
 from rpt.v_order_issue_metrics_summary as cms
 left join rtrbi.order_group_reservation as ogr
 on cms.uid = ogr.uid
 group by 1,2,3,4,5,6,7;
 
 GRANT ALL ON VIEW rpt.v_loyalty_metrics_QUALITY TO tableau_role;
 
  CREATE OR REPLACE VIEW rpt.v_loyalty_metrics_FIT as
 select 
 cms.uid
 , cms.email
 , cms.rental_begin_date
 , CASE WHEN FIT_ORDER_ID IS NOT NULL THEN 'FIT' ELSE 'NON_FIT' END AS order_issue
 , CASE WHEN (FIT_ORDER_ID IS NOT NULL AND RPL_FIT_ORDER_ID IS NOT NULL ) THEN 'W. Replacement'
ELSE 'No Replacement' END AS replacement_flg
 , cms.nth_order
 , CASE WHEN refund_amount = 0 THEN 'no_refund' ELSE 'refund' END refund_flg
 , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 30, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_30d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 60, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_60d
  , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 90, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_90d
   , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 180, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_180d
      , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 270, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_270d
     , count(distinct case when ogr.order_datetime_est between cms.rental_begin_date and dateadd(DAY, 365, cms.rental_begin_date)
 then ogr.order_id end) as orders_in_365d
 from rpt.v_order_issue_metrics_summary as cms
 left join rtrbi.order_group_reservation as ogr
 on cms.uid = ogr.uid
 group by 1,2,3,4,5,6,7;
 
 GRANT ALL ON VIEW rpt.v_loyalty_metrics_FIT TO tableau_role;
 
 SELECT * FROM analytics.ogr_po WHERE po_category IS NOT NULL AND extra_reservation_booking_id IS NOT null;
 
 SELECT * FROM rtrbi.fulfillment WHERE booking_id = 75849274 ORDER BY id,seq;
 
 USE RTR_PROD;
 select * from rtrbi.order_group_reservation LIMIT 2 ;
 WITH rpl as
(
	SELECT  DISTINCT af.order_id
	, ogr.group_id
	, af.annotation_target_id
	, af.detail_value
	FROM etl.yp_autumn_full as af
	INNER JOIN rtrbi.order_group_reservation ogr
	ON af.annotation_target_id = ogr.reservation_booking_id
	where af.annotation_group = 'REPLACEMENT'
	and af.detail_name = 'reason' )
select DISTINCT dt.fiscal_week_start
--, count(DISTINCT ogr.group_id) cnt
, ogr.order_datetime_est
, ogr.order_id
, ogr.group_id
, ogr.rental_begin_date
, ogr.last_possible_begin_date 
,ogr.ups_ground_shipping_zone
from rtrbi.order_group_reservation ogr
left JOIN analytics.ogr_po po 
ON ogr.reservation_booking_id = po.reservation_booking_id
LEFT JOIN rpl 
ON rpl.group_id = ogr.group_id
INNER JOIN rtrbi.dates dt 
ON ogr.order_datetime_est::date = dt.asofdate
where ogr.order_type = 'QUEUE'
and ogr.merchandise_category = 'RENTAL'
and ogr.ups_ground_shipping_zone = 2
--AND po.po_category IS NULL
--AND po.cancel_flag = 0
AND rpl.group_id IS null
and datediff('day',ogr.rental_begin_date, ogr.last_possible_begin_date) =-1
AND dt.fiscal_week_start>='2018-05-10'
--GROUP BY 1
ORDER BY 1 DESC
;
