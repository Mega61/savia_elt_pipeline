{{
    config(
        materialized="incremental",
        unique_key="order_item_id",
    )
}}

with orders as (
    SELECT 
        *,
        TIMESTAMP_SUB(created_at, INTERVAL 5 HOUR) AS created_at_col,
        TIMESTAMP_SUB(promised_delivery_date_time, INTERVAL 5 HOUR) AS promised_delivery_date_time_col
    FROM {{source('mongo_savia_core_qa', 'order')}}
),

order_items as (
    select 
        *,
        (price_before_tax * quantity) - ((price_before_tax * quantity) * (discount/100)) AS net_value,
        (price_before_tax * quantity) * (discount/100) as net_discount,
        TIMESTAMP_SUB(updated_at, INTERVAL 5 HOUR) AS updated_at_col
    from {{source('mongo_savia_core_qa', 'order_item')}}
    where active = true
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
    o.created_at_col,
    o.total_before_tax AS order_total_before_tax,
    o.total_after_tax AS order_total_after_tax,
    o.total_units AS order_total_units,
    p.name AS product_name,
    oi.quantity AS order_item_quantity,
    oi.price_before_tax AS order_item_price_before_tax,
    oi.price_after_tax AS order_item_price_after_tax,
    oi.tax AS order_item_tax,
    oi.discount as order_item_discount,
    oi.net_value,
    oi.net_discount,
    p.sku,
    p.active AS product_active,
    p.price_before_tax AS product_price_before_tax,
    p.price_after_tax AS product_price_after_tax,
    REPLACE(JSON_EXTRACT(o.status, '$.description'), '"','') AS status_description,
    REPLACE(JSON_EXTRACT(o.channel, '$.description'), '"','') AS channel_description,
    o.promised_delivery_date_time_col,
    o.order_number_shopify,
    oi.updated_at_col
from orders o
inner join
order_items oi ON o._id = oi.order_id
inner join
products p ON oi.product_id = p._id
where
o.company_id = "629ec6e7541abf7b407b0ade"
{% if is_incremental() %}
    AND oi.updated_at_col > (select max(updated_at_col) from {{ this }})
{% endif %}