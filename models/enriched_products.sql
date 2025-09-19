-- Enriched products with supply cost and profitability
-- Joins product catalog with supply costs for basic margin analysis

with product_supply_costs as (
    select 
        product_id,
        sum(supply_cost) as total_supply_cost
    from {{ ref('data_eng_project', 'supplies') }}
    group by 1
)

select 
    p.product_id,
    p.product_name,
    p.product_type,
    p.product_description,
    p.product_price,
    p.is_food_item,
    p.is_drink_item,
    
    -- Add supply cost data
    coalesce(sc.total_supply_cost, 0) as supply_cost,
    round(p.product_price - coalesce(sc.total_supply_cost, 0), 2) as unit_profit,
    round((p.product_price - coalesce(sc.total_supply_cost, 0)) / nullif(p.product_price, 0) * 100, 1) as profit_margin_pct,
    
    -- Price tier
    case 
        when p.product_price >= 8 then 'Premium'
        when p.product_price >= 5 then 'Standard'
        else 'Value'
    end as price_tier,
    
    -- Profitability flag
    case 
        when (p.product_price - coalesce(sc.total_supply_cost, 0)) / nullif(p.product_price, 0) >= 0.4 then 'High Margin'
        when (p.product_price - coalesce(sc.total_supply_cost, 0)) / nullif(p.product_price, 0) >= 0.2 then 'Good Margin'
        else 'Low Margin'
    end as margin_tier

from {{ ref('data_eng_project', 'products') }} p
left join product_supply_costs sc on p.product_id = sc.product_id
