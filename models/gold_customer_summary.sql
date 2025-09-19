-- Gold layer: Customer summary aggregations
-- Creates reporting-ready customer metrics from silver layer

select 
    customer_id,
    customer_name,
    customer_type,
    spend_tier,
    recency_status,
    
    -- Order aggregations
    count(*) as total_orders_in_period,
    round(avg(order_total), 2) as avg_order_value_period,
    round(sum(order_total), 2) as total_spent_period,
    round(sum(gross_profit), 2) as total_profit_generated,
    round(avg(margin_percent), 1) as avg_margin_percent,
    
    -- Order behavior patterns  
    count(case when order_size = 'Large' then 1 end) as large_orders,
    count(case when order_type = 'Combo' then 1 end) as combo_orders,
    count(case when order_vs_customer_avg = 'Above Average' then 1 end) as above_avg_orders,
    
    -- Customer context (from enriched customers)
    max(count_lifetime_orders) as lifetime_orders_total,
    max(lifetime_spend_pretax) as lifetime_spend_total,
    max(customer_tenure_days) as customer_tenure_days,
    
    -- Date range
    min(ordered_at) as first_order_in_period,
    max(ordered_at) as last_order_in_period

from {{ ref('silver_customer_orders') }}
group by 1, 2, 3, 4, 5
order by total_spent_period desc
