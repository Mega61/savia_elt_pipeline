{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}
select *
from {{ source('mongo_savia_core_qa', 'order_item') }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}


  