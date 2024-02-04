{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}

    
select _id, company_id, invoice_id, total_units, status
from {{source('mongo_savia_core_qa', 'order')}}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

