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
        TIMESTAMP_SUB(promised_delivery_date_time, INTERVAL 5 HOUR) AS promised_delivery_date_time_col,
        TIMESTAMP_SUB(updated_at, INTERVAL 5 HOUR) AS updated_at_col_order,
        TIMESTAMP_SUB(actual_delivery_date_time, INTERVAL 5 HOUR) AS actual_delivery_date_time_col,
        TIMESTAMP_SUB(invoiced_date_time, INTERVAL 5 HOUR) AS invoiced_date_time_col
    FROM {{source('mongo_savia_core_qa', 'order')}}
),

order_items as (
    select 
        *,
        (price_before_tax * quantity) - ((price_before_tax * quantity) * (discount/100)) AS net_value,
        (price_before_tax * quantity) * (discount/100) as net_discount,
        TIMESTAMP_SUB(updated_at, INTERVAL 5 HOUR) AS updated_at_col
    from {{source('mongo_savia_core_qa', 'order_item')}}
),

products as (
    select *
    from {{source('mongo_savia_core_qa', 'product')}}
),

vendors as (
    select u._id, u.full_name, u.primary_email, u.last_login, ot.order_id
    from {{source('mongo_savia_core_qa', 'user')}} u
    join {{source('mongo_savia_core_qa', 'order_tracking')}} ot on u._id = ot.user_id
    join {{source('mongo_savia_core_qa', 'status')}} s on s._id = ot.status_id
    where s.role = "START"
)

select
    oi._id AS order_item_id,
    REPLACE(JSON_QUERY(o.customer, '$.fullName'), '"','') AS customer_full_name,
    REPLACE(JSON_QUERY(o.customer, '$.idType'), '"','') AS customer_id_type,
    REPLACE(JSON_QUERY(o.customer, '$.identification'), '"','') AS customer_identification,
    REPLACE(JSON_QUERY(o.shipping_address, '$.city.name'), '"','') AS shipping_city,
    REPLACE(JSON_QUERY(o.shipping_address, '$.state.name'), '"','') AS shipping_state,
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
    oi.net_value / sum(oi.net_value) OVER (PARTITION BY EXTRACT(YEAR FROM o.created_at_col), EXTRACT(MONTH FROM o.created_at_col)) * 100 AS net_value_percetange,
    oi.net_discount,
    o.invoicing_status,
    o.invoiced_date_time_col,
    p.sku,
    p.active AS product_active,
    p.price_before_tax AS product_price_before_tax,
    p.price_after_tax AS product_price_after_tax,
    REPLACE(JSON_QUERY(o.status, '$.description'), '"','') AS status_description,
    REPLACE(JSON_QUERY(o.channel, '$.description'), '"','') AS channel_description,
    o.promised_delivery_date_time_col,
    o.actual_delivery_date_time_col,
    o.order_number_shopify,
    o.is_paid,
    o.invoice_id,
    oi.updated_at_col,
    o.updated_at_col_order,
    oi.active,
    v._id AS vendor_id,
    v.full_name AS vendor_full_name,
    v.primary_email AS vendor_primary_email,
    v.last_login AS vendor_last_login
from orders o
inner join
order_items oi ON o._id = oi.order_id
inner join
products p ON oi.product_id = p._id
left join
vendors v ON v.order_id = o._id
where
o.company_id = "629ec6e7541abf7b407b0ade"
{% if is_incremental() %}
    AND o.updated_at_col_order > (select max(updated_at_col_order) from {{ this }})
{% endif %}