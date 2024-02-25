{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}

with orders as (
    SELECT *
    FROM {{SOURCE('savia_savia_core_pdn', 'order')}}
    {% if is_incremental() %}
        AND updated_at > (select max(updated_at) from {{ this }} WHERE source = 'order')
    {% endif %}
),

order_items as (
    select *
    from {{SOURCE('savia_savia_core_pdn', 'order_item')}}
    {% if is_incremental() %}
        AND updated_at > (select max(updated_at) from {{ this }} WHERE source = 'order_item')
    {% endif %}
),

products as (
    select *
    from {{SOURCE('savia_savia_core_pdn', 'product')}}
    {% if is_incremental() %}
        AND updated_at > (select max(updated_at) from {{ this }} WHERE source = 'order_item')
    {% endif %}
)

select 
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
    AND (o.updated_at > (select max(updated_at) from {{ this }})
    OR oi.updated_at > (select max(updated_at) from {{ this }})
    OR p.updated_at > (select max(updated_at) from {{ this }}))
{% endif %}