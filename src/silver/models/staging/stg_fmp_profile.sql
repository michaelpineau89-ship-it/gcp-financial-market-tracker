with source as (
    select * from {{ source('bronze', 'bronze_fmp_profile') }}
),

deduped as (
    select *,
        row_number() over (
            partition by symbol
            order by _ingested_at desc
        ) as _row_num
    from source
    where symbol is not null
)

select
    symbol                          as ticker,
    companyName                     as company_name,
    sector,
    industry,
    country,
    exchange,
    cast(mktCap as INT64)           as market_cap,
    cast(price as FLOAT64)          as price,
    cast(beta as FLOAT64)           as beta,
    _ingested_at
from deduped
where _row_num = 1
