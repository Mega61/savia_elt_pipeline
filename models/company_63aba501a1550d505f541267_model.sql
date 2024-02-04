{{
    config(
        materialized="incremental",
        unique_key="_id",
    )
}}

    
select _id, company_id, invoice_id, total_units, status
from {{ref("customer_segregation_model")}}
where company_id = "63aba501a1550d505f541267"
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}