/*-------------------------------------------------------------------------------------------
 * 	NOTE: RUN ALL OF THESE ANALYSIS CHUNKS INDIVIDUALLY! 
 * 	FORMATTING OF EACH QUERY IS DIFFERENT, THIS FILE WILL ERROR IF IT'S RUN ALL AT ONCE!
 * -------------------------------------------------------------------------------------------
 */

/* ===============================================================
 * Change over time Analysis: Tracking data over time
 * ===============================================================
 */
select 
	cast(extract(year from order_date)as varchar) as year_date,
	extract(month from order_date) as month_date,
	sum(sales_amount) as total_sales,
	-- optionally mapping month numbers to simpler values  
	/* concat(extract(month from order_date), ' ', '(', case extract(month from order_date) 
	when 1 then 'Jan'
	when 2 then 'Feb' 
	when 3 then 'Mar'
	when 4 then 'Apr'
	when 5 then 'May'
	when 6 then 'Jun'
	when 7 then 'Jul'
	when 8 then 'Aug'
	when 9 then 'Sep'
	when 10 then 'Oct'
	when 11 then 'Nov'
	when 12 then 'Dec'
	end, ')') as month_order */ 
	count(distinct customer_key) as total_customers,
	sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year_date, month_date
order by year_date, month_date; 

/* ===============================================================
 * Cumulative Analysis: Progressively aggregating data over time
 * Aids in understanding growth/declination in the business
 * ===============================================================
 */

-- total sales per month and the running total of sales over time 
select 
order_date, 
total_sales, 
sum(total_sales) over(order by order_date asc) as running_total_sales,
avg(avg_price) over(order by order_date asc) as moving_average_price 
from(
select 
	cast(date_trunc('month', order_date) as date) as order_date,
	sum(sales_amount) as total_sales,
	avg(price) as avg_price 
from gold.fact_sales 
where order_date is not null
group by cast(date_trunc('month', order_date) as date)
order by order_date
)t;

/* ===============================================================
 * Performance Analysis: Comparing the current values to goal 
 * values. Helps us guage performance. 
 * ===============================================================
 */
-- analyzing the yearly performance of products by comparing each product's sales to both its average sales performance and the previous year's sales
with yearly_product_sales as(
select 
cast(extract(year from f.order_date) as varchar) as order_year,
p.product_name, 
sum(f.sales_amount) as current_sales
from gold.fact_sales as f 
left join gold.dim_products p 
on f.product_key = p.product_key
where f.order_date is not null
group by extract(year from f.order_date), p.product_name
order by extract(year from f.order_date) asc
)
select 
order_year, 
product_name, 
current_sales,
avg(current_sales) over(partition by product_name) avg_sales, 
current_sales - avg(current_sales) over(partition by product_name) as diff_avg, 
case when current_sales - avg(current_sales) over(partition by product_name) > 0 then 'Above Avg'
when current_sales - avg(current_sales) over(partition by product_name) < 0 then 'Below Avg'
else 'Avg'
end as avg_change, 
-- year over year analysis, can be scaled to month over month analysis (long-term or short-term, seasonality)
lag(current_sales) over(partition by product_name order by order_year asc) as py_sales, 
current_sales - lag(current_sales) over(partition by product_name order by order_year asc) as diff_py, 
case when current_sales - lag(current_sales) over(partition by product_name order by order_year asc) > 0 then 'Increasing'
when current_sales - lag(current_sales) over(partition by product_name order by order_year asc) < 0 then 'Decreasing'
else 'No Change'
end py_change
from yearly_product_sales
order by product_name, order_year;

/* ===============================================================
 * Proportional Analysis: Analyzing how an individual part is
 * performing compared to the overall whole, pinpointing impact
 * ===============================================================
 */
-- Finding categories that contribute the most to overall sales
select 
category, 
total_category_sales, 
total_sales, 
cast(total_category_sales as float) / cast(total_sales as float) * 100 as percentage_of_total
from (
select 
distinct p.category, 
sum(f.sales_amount) over(partition by p.category) as total_category_sales, 
sum(f.sales_amount) over() as total_sales
from gold.fact_sales f 
left join gold.dim_products p 
on f.product_key = p.product_key
)

-- using a CTE 
with category_sales as (
select 
dp.category, 
sum(sales_amount) total_sales 
from gold.fact_sales t 
left join gold.dim_products dp 
on dp.product_key = t.product_key 
group by dp.category
) 
select
category,
total_sales, 
sum(total_sales) over() as overall_sales, 
round(total_sales/sum(total_sales) over() * 100,2) as percentage_of_total
from category_sales
order by total_sales desc


/* ===============================================================
 * Data Segmentation: Group that data based on a specific range. 
 * Helps understand the correlation between two measures. 
 * ===============================================================
 */ 
-- segmenting products into cost ranges and counting how many products fall into each segment 
-- plan: establish cost ranges first, then cte for counting amount of products in those ranges 
with product_segments as(
select 
product_key, 
product_name, 
product_cost as cost, 
case when product_cost < 100 then 'Below 100' 
when product_cost between 100 and 500 then '100-500'
when product_cost between 500 and 1000 then '500-1000'
else 'Above 1000'
end cost_range
from gold.dim_products
) 
select 
distinct cost_range,
count(product_key) over(partition by cost_range) as num_products_per_range
from product_segments
order by num_products_per_range desc; 

/* Grouping customers into three segments based on their spending behavior: 
 * VIP: at least 12 months of history and spending more than $5000
 * Regular: at least 12 months of history but spending $5000 or less 
 * New: lifespan of less than 12 months */ 
select 
distinct customer_segment,
count(customer_key) over(partition by customer_segment) as total_customers_by_segment
from(
with customer_spending as(
select 
c.customer_key, 
sum(t.sales_amount) as total_spending,
min(t.order_date) first_order, 
max(t.order_date) last_order, 
(max(t.order_date) - min(t.order_date)) / 30 as lifespan 
from gold.fact_sales t 
left join gold.dim_customers as c on c.customer_key = t.customer_key
group by c.customer_key
) 
select 
customer_key, 
total_spending, 
lifespan,
case when lifespan >= 12 and total_spending > 5000 then 'VIP' 
when lifespan >= 12 and total_spending <=5000 then 'Regular' 
else 'New' 
end as customer_segment
from customer_spending
)
order by total_customers_by_segment desc
