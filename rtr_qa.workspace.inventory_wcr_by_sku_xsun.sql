create or replace table rtr_qa.workspace.inventory_wcr_by_sku_xsun as
with 
revenue_cogs as
(SELECT
FISCAL_YEAR_RBD 
,FISCAL_MONTH_RBD 
,FISCAL_MONTH_START_RBD 
,FISCAL_MONTH_END_RBD 
,FISCAL_YEAR_AVL
,FISCAL_MONTH_AVL 
,FISCAL_MONTH_START_AVL 
,FISCAL_MONTH_END_AVL 
,a.STYLE 
,a.SKU 
,a.COST 
,a.TYPE 
,a.IS_BOUGHT_FOR_UNLIMITED 
,a.ORDER_TYPE
,INITIAL_TOTAL_UNITS 
,CURRENT_TOTAL_UNITS 
,MONTHLY_TOTAL_UNITS 
,SHORTSHIP_RTV_MONTHLY
,SHORTSHIP_RTV_INITIAL 
,SHORTSHIP_RTV_TOTAL 
,RENTAL_PRICE 
,TOTAL_BOOKING 
,TOTAL_ORDERS 
,ORDER_TOTAL_INC_TAX_SKU
,ORDER_TOTAL_EXC_TAX_SKU 
,NON_RENTAL_PRODUCT_TOTAL_SKU 
,PRODUCT_TOTAL_SKU 
,INSURANCE_TOTAL_SKU 
,TAX_TOTAL_SKU 
,SHIPPING_TOTAL_SKU 
,COUPON_TOTAL_SKU 
,CREDIT_REFUND_TOTAL_SKU 
,CASH_REFUND_TOTAL_SKU 
,UPR_CREDIT_REDEEMED_SKU 
,UPR_REFUND_CASH_SKU 
,UPR_REFUND_CREDIT_SKU 
,FLI_REFUND_CASH_SKU
,FLI_REFUND_CREDIT_SKU 
,FLI_TOTAL_CASH_IN_SKU 
,FLI_TOTAL_CREDIT_REDEEMED_SKU 
,FLI_TOTAL_REVENUE_SKU
,QUEUE_SUBSCRIPTION_REVENUE_SKU 
,QUEUE_TTB_REVENUE_SKU 
,queue_monthly_athome_days_sku 
,FULFILLMENT_COGS_SHIPPING_OB_SKU 
,FULFILLMENT_COGS_SHIPPING_IB_SKU 
,FULFILLMENT_COGS_LABOR_SKU 
,FULFILLMENT_COGS_OTHER_SKU 
,designer 
,title
,sub_type 
,high_level_shape 
,detailed_formality 
,designer_classification 
,season_code 
,style_out
,replenishment_style
from rtrbi.profitability_sku_monthly_revenue_cogs as a
inner join analytics.products as p 
on a.style = p.style
left join (
select style as replenishment_style
from analytics.master_replenish_style
union 
	select pm.model as replenishment_style
	from etl.products_master as pm
	where pm.is_replenishment_style = 1
) replenishment_style_list
on a.style = replenishment_style_list.replenishment_style
where a.fiscal_year_avl >= 2009)
, wcr as(
select  distinct 
fiscal_year_avl
, sku
, fiscal_month_start_rbd as fiscal_month_start
, datediff('month',cast(concat(concat(concat(cast(fiscal_year_avl as varchar(4)),'-'),fiscal_month_avl),'-01') as datetime)
,cast(concat(concat(concat(cast(year(fiscal_month_start_rbd) as varchar(4)),'-'),fiscal_month_rbd),'-01') as datetime)) month_since_available
,CURRENT_TOTAL_UNITS as unit_count
, cost*CURRENT_TOTAL_UNITS as wholesale_cost      
,sum(product_total_sku) rev_reserve
, sum(queue_subscription_revenue_sku) as rev_subscription
, sum(QUEUE_TTB_REVENUE_SKU ) as rev_liquidation
, sum(case when order_type in ('CLASSIC_POSH','STORE_PICKUP','CLASSIC','STORE_PICKUPWEB') then total_booking end) as order_reserve
, sum(queue_monthly_athome_days_sku ) as subscription_athome_days
from revenue_cogs 
where fiscal_year_avl in (2015,2016,2017,2018)
and replenishment_style is null
group by 1,2,3,4,5,6)
select
a1.*
,sum(a.rev_reserve+a.rev_subscription+a.rev_liquidation) as rev_running_total
,sum(a.rev_reserve+a.rev_subscription+a.rev_liquidation)/(a1.wholesale_cost+0.01)-1 as wcr_pct
from wcr as a
inner join wcr as a1
on a.sku =a1.sku
and a.month_since_available<=a1.month_since_available
group by 1,2,3,4,5,6,7,8,9,10,11;
