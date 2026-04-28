with source as (
    select * from {{ source('bronze', 'bronze_edgar_submissions') }}
),

deduped as (
    select *,
        row_number() over (
            partition by cik
            order by _ingested_at desc
        ) as _row_num
    from source
    where cik is not null
)

select
    cast(cik as STRING)         as cik,
    entity_name,
    ticker,
    entity_type,
    cast(sic as STRING)         as sic_code,
    sic_description,
    fiscal_year_end,
    cast(is_whale as BOOL)      as is_whale,
    _ingested_at
from deduped
where _row_num = 1
