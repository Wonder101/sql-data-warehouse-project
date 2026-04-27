/* 
========================================================================
Customer Report 
========================================================================
Purpose: 
	- This report consolidates key customer metrics and behaviors 

Highlights: 
 	1. Gathers essential fields such as names, ages, and transaction details. 
	2. Segments customers into categories (VIP, Regular, New) and age groups. 
	3. Aggregates customer-level metrics: 
	- total orders 
	- total sales 
	- total quantity purchased 
	- total products 
	- lifespan (in months) 
4. Calculates valuable KPIs: 
	- recency (months since last order) 
	- average order value 
	- average monthly spend 
======================================================================== 
*/ 
create view gold.report_customers as 
-- 1) Base Query: Retrieving core columns from tables 
with base_query as (
select 
t.order_number, 
t.product_key, 
t.order_date, 
t.sales_amount, 
t.quantity, 
dc.customer_key, 
dc.customer_number, 
concat(dc.first_name, ' ', dc.last_name) as customer_name, 
extract (year from age(current_date, dc.birthdate)) as age
from gold.fact_sales t 
left join gold.dim_customers dc 
on dc.customer_key = t.customer_key 
where t.order_date is not null 
) 
, -- 2) Customer aggregations: Summarizing key metrics at the customer level
customer_aggregation as (
select 
customer_key, 
customer_number, 
customer_name, 
age, 
count(distinct order_number) as total_orders, 
sum(sales_amount) as total_sales, 
sum(quantity) as total_quantity, 
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
(max(order_date) - min(order_date)) / 30 as lifespan 
from base_query
group by customer_key, customer_number, customer_name, age
)
select 
customer_key, 
customer_number, 
customer_name, 
case when age < 20 then 'Under 20'
	when age between 20 and 29 then '20-29'
	when age between 30 and 39 then '30-39'
	when age between 40 and 49 then '40-49'
	else '50 and above' 
end as age_group,
case when lifespan >= 12 and total_sales > 5000 then 'VIP' 
when lifespan >= 12 and total_sales <=5000 then 'Regular' 
else 'New' 
end as customer_segment,
(current_date - last_order_date) / 30 as recency,
total_orders, 
total_sales, 
total_quantity, 
total_products,  
lifespan, 
-- average order value (AVO)
case when total_orders = 0 then 0 
else total_sales/ total_orders  
end as avg_order_value,
-- average monthly spending
case when lifespan = 0 then total_sales 
else total_sales / lifespan 
end as avg_monthly_spending
from customer_aggregation

