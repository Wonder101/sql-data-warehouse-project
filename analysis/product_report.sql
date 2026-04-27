/* 
=======================================================================================================
Product Report
=======================================================================================================
Purpose: 
	- This report consolidates key product metrics and behaviors.

Highlights: 
 	1. Gathers essential fields such as product name, category, subcategory, and cost
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers
	3. Aggregates product-level metrics: 
	 	- total orders 
	 	- total sales 
	 	- toatl quantity sold 
	 	- total customers (unique) 
	 	- lifespan (in months) 
	- lifespan (in months) 
4. Calculates valuable KPIs: 
	- recency (months since last sale) 
	- average order revenue (AOR) 
	- average monthly revenue  
=======================================================================================================
*/ 
create view gold.report_products as 
with base_query as (
select
f.order_number, 
f.order_date, 
f.customer_key, 
f.sales_amount, 
f.quantity, 
p.product_key, 
p.product_name, 
p.category, 
p.subcategory, 
p.product_cost 
from gold.fact_sales f 
left join gold.dim_products p 
on f.product_key = p.product_key 
where order_date is not null
)
,
product_aggregations as (
select 
product_key, 
product_name, 
category, 
subcategory, 
product_cost, 
(max(order_date) - min(order_date)) / 30 as lifespan, 
max(order_date) as last_sale_date, 
count(distinct order_number) as total_orders, 
count(distinct customer_key) as total_customers, 
sum(sales_amount) as total_sales, 
sum(quantity) as total_quantity, 
round(avg(cast(sales_amount as numeric)/nullif(quantity, 0)),1) as avg_selling_price
from base_query 
group by
product_key, 
product_name, 
category, 
subcategory, 
product_cost
)
select 
product_key, 
product_name, 
category, 
subcategory, 
product_cost, 
last_sale_date, 
current_date - last_sale_date as recency_in_months, 
case when total_sales > 50000 then 'High Performer'
when total_sales >= 10000 then 'Mid Range'
else 'Low Performer'
end as product_segment,
lifespan, 
total_orders, 
total_sales, 
total_quantity, 
total_customers, 
avg_selling_price, 
-- average order revenue (AOR)
case when total_orders = 0 then 0 
else total_sales/total_orders 
end as avg_order_revenue, 
-- average monthly revenue 
case when lifespan = 0 then total_sales 
else total_sales/lifespan
end as avg_monthly_revenue 
from product_aggregations 
