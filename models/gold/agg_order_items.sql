with aggregated_order_items as (
    select 
        order_id,
        count(order_item_id) as total_items,
        sum(product_price) as total_revenue,
        sum(supply_cost) as total_supply_cost,
        sum(item_profit) as total_profit,
        round(avg(item_margin_pct), 1) as avg_margin_pct,
        sum(case when is_food_item then 1 else 0 end) as total_food_items,
        sum(case when is_drink_item then 1 else 0 end) as total_drink_items
    from {{ ref('enriched_order_items') }}
    group by order_id
)

select * from aggregated_order_items