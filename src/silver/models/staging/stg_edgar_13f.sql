with source as (
    select * from {{ source('bronze', 'bronze_edgar_13f') }}
),

deduped as (
    select *,
        row_number() over (
            partition by cik, cusip
            order by _ingested_at desc
        ) as _row_num
    from source
    where cik is not null and cusip is not null
)

select
    cast(cik as STRING)                               as cik,
    entity_name,
    nameOfIssuer                                      as issuer_name,
    titleOfClass                                      as class_title,
    cusip,
    cast(value as INT64)                              as value_thousands_usd,
    cast(value as INT64) * 1000                       as value_usd,
    shrsOrPrnAmt                                      as shares_or_principal_amount,
    _ingested_at
from deduped
where _row_num = 1
