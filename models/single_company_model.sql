{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}    
select
            *
from {{source('mongo_savia_core_qa', 'order')}}
where company_id = "629ec6e7541abf7b407b0ade"
{% if is_incremental() %}
    AND updated_at > (select max(updated_at) from {{ this }})
{% endif %}