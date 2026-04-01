with aggregated_order_items as (
    select 
        order_id,
        count(order_item_id) as total_items,
        sum(product_price) as total_revenue,
        sum(supply_cost) as total_supply_cost,
        sum(item_profit) as total_profit,
        avg(item_margin_pct) as avg_margin_pct,
        max(ordered_at) as last_ordered_at
    from {{ ref('enriched_order_items') }}
    group by order_id
)

select * from aggregated_order_items