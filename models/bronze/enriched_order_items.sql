-- Enriched order items with profit calculations
-- Adds item-level profitability using supply costs

select 
    order_item_id,
    order_id,
    product_id,
    ordered_at,
    product_name,
    product_price,
    is_food_item,
    is_drink_item,
    supply_cost,
    
    -- Calculate item profitability
    round(product_price - supply_cost, 2) as item_profit,
    round((product_price - supply_cost) / nullif(product_price, 0) * 100, 1) as item_margin_pct,
    
    -- Item performance flags
    case 
        when (product_price - supply_cost) / nullif(product_price, 0) >= 0.5 then 'High Profit'
        when (product_price - supply_cost) / nullif(product_price, 0) >= 0.3 then 'Good Profit'
        else 'Low Profit'
    end as profit_tier

from {{ ref('data_eng_project', 'order_items') }}
