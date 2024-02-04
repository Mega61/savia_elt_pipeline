with source as (
      select * from {{ source('savia_processed_data', 'customers_compression') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  