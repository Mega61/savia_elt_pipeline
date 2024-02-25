{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}

with orders as (
    SELECT *
    FROM {{source('mongo_savia_core_qa', 'order')}}
),

order_items as (
    select *
    from {{source('mongo_savia_core_qa', 'order_item')}}
),

products as (
    select *
    from {{source('mongo_savia_core_qa', 'product')}}
)

select
    oi._id,
    o.order_number,
    p.name,
    oi.quantity,
    o.updated_at
from orders o
inner join
order_items oi ON o._id = oi.order_id
inner join
products p ON oi.product_id = p._id
where
o.company_id = "629ec6e7541abf7b407b0ade"
{% if is_incremental() %}
    AND o.updated_at > (select max(updated_at) from {{ this }})
{% endif %}