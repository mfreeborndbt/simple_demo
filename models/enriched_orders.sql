-- Enriched orders with basic calculated fields
-- Adds order size categories and margin calculations

select 
    order_id,
    customer_id,
    location_id,
    order_total,
    ordered_at,
    order_cost,
    tax_paid,
    subtotal,
    is_food_order,
    is_drink_order,
    
    -- Add basic enrichments
    round(order_total - order_cost, 2) as gross_profit,
    round((order_total - order_cost) / nullif(order_total, 0) * 100, 1) as margin_percent,
    
    -- Order size category
    case 
        when order_total >= 25 then 'Large'
        when order_total >= 15 then 'Medium'
        else 'Small'
    end as order_size,
    
    -- Order type classification
    case 
        when is_food_order and is_drink_order then 'Combo'
        when is_food_order then 'Food Only'
        when is_drink_order then 'Drink Only'
        else 'Other'
    end as order_type,
    
    -- Time of day (simplified)
    extract(hour from ordered_at) as order_hour

from {{ ref('data_eng_project', 'orders') }}
