{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}

    
select _id, company_id, invoice_id, total_units, status
from {{ref("customer_segregation_model")}}
where company_id = "629ec6e7541abf7b407b0ade"
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}