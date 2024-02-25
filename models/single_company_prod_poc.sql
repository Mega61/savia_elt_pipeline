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
    oi._id AS order_item_id,
    REPLACE(JSON_EXTRACT(o.customer, '$.fullName'), '"','') AS customer_full_name,
    REPLACE(JSON_EXTRACT(o.customer, '$.idType'), '"','') AS customer_id_type,
    REPLACE(JSON_EXTRACT(o.customer, '$.identification'), '"','') AS customer_identification,
    o.order_number,
    o.created_at,
    o.total_before_tax AS order_total_before_tax,
    o.total_after_tax AS order_total_after_tax,
    o.total_units AS order_total_units,
    p.name AS product_name,
    oi.quantity AS order_item_quantity,
    oi.price_before_tax AS order_item_price_before_tax,
    oi.price_after_tax AS order_item_price_after_tax,
    oi.tax AS order_item_tax,
    p.sku,
    p.active AS product_active,
    p.price_before_tax AS product_price_before_tax,
    p.price_after_tax AS product_price_after_tax,
    REPLACE(JSON_EXTRACT(o.status, '$.description'), '"','') AS status_description,
    REPLACE(JSON_EXTRACT(o.channel, '$.description'), '"','') AS channel_description,
    o.promised_delivery_date_time,
    o.order_number_shopify,
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