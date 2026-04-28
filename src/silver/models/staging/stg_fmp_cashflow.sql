with source as (
    select * from {{ source('bronze', 'bronze_fmp_cashflow') }}
),

deduped as (
    select *,
        row_number() over (
            partition by symbol, date, period
            order by _ingested_at desc
        ) as _row_num
    from source
    where symbol is not null and date is not null
)

select
    symbol                                     as ticker,
    cast(date as DATE)                         as period_end_date,
    period,
    cast(operatingCashFlow as INT64)           as operating_cash_flow,
    cast(capitalExpenditure as INT64)          as capital_expenditure,
    cast(freeCashFlow as INT64)                as free_cash_flow,
    cast(dividendsPaid as INT64)               as dividends_paid,
    _ingested_at
from deduped
where _row_num = 1
