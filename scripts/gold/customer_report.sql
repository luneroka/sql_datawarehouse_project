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

/* ---------------------------------------------------------------------
1) Base Query : Retrieve core columns from tables
2) Transform relevant columns for reporting clarity (name, age, etc.
--------------------------------------------------------------------- */
create view gold.report_customers as
with base_query as (
	select 
		f.order_number,
		f.product_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		dc.customer_key,
		dc.customer_number,
		concat(dc.first_name, ' ', dc.last_name) as customer_name,
		extract(year from age(current_date, birthdate)) as customer_age
	from gold.fact_sales f
	left join gold.dim_customers dc 
		on f.customer_key = dc.customer_key 
	where order_date is not null
)
/* ---------------------------------------------------------------------
3) Aggregations : build another CTE derived from the base_query CTE
--------------------------------------------------------------------- */
, customer_aggregation as (
select
	customer_key,
	customer_number,
	customer_name,
	customer_age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct product_key) as total_products,
	max(order_date) as last_order_date,
	extract(year from age(max(order_date), min(order_date))) * 12
		+ extract(month from age(max(order_date), min(order_date))) as lifespan
from base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	customer_age
)
/* ---------------------------------------------------------------------
4) Final results + calculate valuable KPIs (recency, etc.)
--------------------------------------------------------------------- */
select
	customer_key,
	customer_number,
	customer_name,
	case
		when customer_age < 20 then 'Under 20'
		when customer_age between 20 and 29 then '20-29'
		when customer_age between 30 and 39 then '30-39'
		when customer_age between 40 and 49 then '20-29'
		else 'Above 50'
	end as age_group,
	case
		when lifespan >= 12 and total_sales > 5000 then 'VIP'
		when lifespan >= 12 and total_sales <= 5000 then 'Regular'
		else 'New'
	end as customer_segment,
	last_order_date,
	extract(year from age(current_date, last_order_date)) * 12
		+ extract(month from age(current_date, last_order_date)) as recency_in_months,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	lifespan,
	case
		when total_orders = 0 then 0
		else total_sales / total_orders
	end as avg_order_value,
	case
		when lifespan = 0 then total_sales
		else round(total_sales / lifespan)
	end as avg_monthly_spend	
from customer_aggregation

select * from gold.report_customers