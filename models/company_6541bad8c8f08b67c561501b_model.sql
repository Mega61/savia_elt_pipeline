{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}

    
select _id, company_id, invoice_id, total_units, status, updated_at
from {{ref("customer_segregation_model")}}
where company_id = "6541bad8c8f08b67c561501b"
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}