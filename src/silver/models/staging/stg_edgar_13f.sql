with source as (
    select * from {{ source('bronze', 'bronze_edgar_13f') }}
),

deduped as (
    select *,
        row_number() over (
            partition by cik, period_of_report, cusip
            order by _ingested_at desc
        ) as _row_num
    from source
    where cik is not null and cusip is not null
)

select
    cast(cik as STRING)                               as cik,
    entity_name,
    cast(filing_date as DATE)                         as filing_date,
    cast(period_of_report as DATE)                    as period_of_report,
    name_of_issuer,
    title_of_class,
    cusip,
    cast(value as INT64)                              as value_thousands_usd,
    cast(value as INT64) * 1000                       as value_usd,
    cast(shares_or_principal_amount as INT64)         as shares,
    _ingested_at
from deduped
where _row_num = 1
