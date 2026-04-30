-- Verifies that every (ticker, period_end_date, period) present in the FMP
-- income statement also exists in the balance sheet and the cash flow
-- statement. A non-empty result means the three statements are not fully
-- joinable on their shared grain key.

with income as (
    select ticker, period_end_date, period
    from {{ ref('stg_fmp_income') }}
),

balance as (
    select ticker, period_end_date, period
    from {{ ref('stg_fmp_balance') }}
),

cashflow as (
    select ticker, period_end_date, period
    from {{ ref('stg_fmp_cashflow') }}
),

income_missing_from_balance as (
    select
        i.ticker,
        i.period_end_date,
        i.period,
        'income_missing_from_balance' as issue
    from income i
    left join balance b
        on  i.ticker          = b.ticker
        and i.period_end_date = b.period_end_date
        and i.period          = b.period
    where b.ticker is null
),

income_missing_from_cashflow as (
    select
        i.ticker,
        i.period_end_date,
        i.period,
        'income_missing_from_cashflow' as issue
    from income i
    left join cashflow c
        on  i.ticker          = c.ticker
        and i.period_end_date = c.period_end_date
        and i.period          = c.period
    where c.ticker is null
)

select * from income_missing_from_balance
union all
select * from income_missing_from_cashflow
