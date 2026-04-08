/*
========================================================================
Product Report
========================================================================

Purpose:
- This report consolidates key product metrics and behaviors.

Highlights:
1. Gathers essential fields such as product name, category, subcategory, and cost.
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
3. Aggregates product-level metrics:
	- total orders
	- total sales
	- total quantity sold
	- total customers (unique)
	- lifespan (in months)
4. Calculates valuable KPIs:
	- recency (months since last sale)
	- average order revenue (AOR)
	- average monthly revenue
========================================================================
*/
create view report_products as
with base_query as (
	select 
		dp.product_key,
		dp.product_name,
		dp.category,
		dp.subcategory,
		dp.cost,
		f.customer_key,
		f.order_number,
		f.sales_amount,
		f.quantity,
		f.order_date 
	from gold.dim_products dp 
	left join gold.fact_sales f
		on dp.product_key = f.product_key 
	where order_date is not null
)
, product_aggregation as (
	select 
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		count(distinct order_number) as total_orders,
		count(quantity) as total_quantity,
		sum(sales_amount) as total_sales,
		round(avg(sales_amount / nullif(quantity, 0)), 2) as avg_selling_price,
		count(distinct customer_key) as total_customers,
		max(order_date) as last_order_date,
		extract(year from age(max(order_date), min(order_date))) * 12
		+ extract(month from age(max(order_date), min(order_date))) as lifespan
	from base_query
	group by 
		product_key,
		product_name,
		category,
		subcategory,
		cost
)
select
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	case
		when total_sales < 50000 then 'Low-Performer'
		when total_sales between 50000 and 100000 then 'Mid-Range'
		else 'High-Performer'
	end as revenue_segment,
	last_order_date,
	extract(year from age(current_date, last_order_date)) * 12
		+ extract(month from age(current_date, last_order_date)) as recency_in_months,
	total_orders,
	total_sales, 
	avg_selling_price,
	total_quantity,
	total_customers,
	case
		when total_orders = 0 then 0
		else total_sales / total_orders 
	end as avg_order_revenue,
	case 
		when lifespan = 0 then total_sales
		else round(total_sales / lifespan)
	end as avg_monthly_revenue
from product_aggregation