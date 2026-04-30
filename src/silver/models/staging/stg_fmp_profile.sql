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
    currency,
    cik,
    isin,
    cusip,
    description,
    ceo,
    cast(fullTimeEmployees as INT64) as full_time_employees,
    phone,
    website,
    address,
    city,
    state,
    zip,
    ipoDate as ipo_date,
    isEtf as is_etf,
    isActivelyTrading as is_actively_trading,
    isAdr as is_adr,
    isFund as is_fund,
    cast(marketCap as INT64)           as market_cap,
    cast(price as FLOAT64)          as price,
    cast(beta as FLOAT64)           as beta,
    cast(lastDividend as FLOAT64)   as last_dividend,
    cast("range" as STRING)            as van_range,
    cast(change as FLOAT64)         as change,
    cast(changePercentage as FLOAT64) as changes_percentage,
    cast(volume as FLOAT64)          as volume,
    cast(averageVolume as INT64)     as average_volume,
    _ingested_at
from deduped
where _row_num = 1
