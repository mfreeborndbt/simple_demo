-- Enriched customers with basic calculated fields
-- Adds tenure, recency, and spend tier classifications

select 
    customer_id,
    customer_name,
    customer_type,
    count_lifetime_orders,
    lifetime_spend_pretax,
    lifetime_spend,
    first_ordered_at,
    last_ordered_at,
    
    -- Calculate basic enrichments
    datediff('day', first_ordered_at, current_date()) as customer_tenure_days,
    datediff('day', last_ordered_at, current_date()) as days_since_last_order,
    round(lifetime_spend_pretax / nullif(count_lifetime_orders, 0), 2) as avg_order_value,
    
    -- Simple spend tier
    case 
        when lifetime_spend_pretax >= 150 then 'High Value'
        when lifetime_spend_pretax >= 75 then 'Medium Value'
        else 'Low Value'
    end as spend_tier,
    
    -- Customer recency status
    case 
        when days_since_last_order <= 30 then 'Active'
        when days_since_last_order <= 90 then 'At Risk'
        else 'Inactive'
    end as recency_status

from {{ ref('data_eng_project', 'customers') }}
