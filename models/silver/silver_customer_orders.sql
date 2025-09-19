-- Silver layer: Customer orders joined with enriched customer data
-- Combines order behavior with customer segments for analysis

select 
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_type,
    c.spend_tier,
    c.recency_status,
    c.avg_order_value as customer_avg_order_value,
    o.order_total,
    o.ordered_at,
    o.gross_profit,
    o.margin_percent,
    o.order_size,
    o.order_type,
    o.order_hour,
    
    -- Customer context
    c.count_lifetime_orders,
    c.lifetime_spend_pretax,
    c.customer_tenure_days,
    
    -- Order vs customer average comparison
    case 
        when o.order_total > c.avg_order_value * 1.5 then 'Above Average'
        when o.order_total < c.avg_order_value * 0.7 then 'Below Average'
        else 'Typical'
    end as order_vs_customer_avg

from {{ ref('enriched_orders') }} o
join {{ ref('enriched_customers') }} c 
    on o.customer_id = c.customer_id
