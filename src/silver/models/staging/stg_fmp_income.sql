with source as (
    select * from {{ source('bronze', 'bronze_fmp_income') }}
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
    symbol                                                      as ticker,
    cast(date as DATE)                                          as period_end_date,
    period,
    reportedCurrency                                            as currency,
    cik,
    acceptedDate as accepted_date,
    fiscalYear as fiscal_year,
    cast(revenue as INT64)                                      as revenue,
    cast(costOfRevenue as INT64)                                as cost_of_revenue,
    cast(grossProfit as INT64)                                  as gross_profit,
    cast(researchAndDevelopmentExpenses as INT64)               as r_and_d_expenses,
    cast(generalAndAdministrativeExpenses as INT64)             as general_and_admin_expenses,
    cast(sellingAndMarketingExpenses as INT64)                  as selling_and_marketing_expenses,
    cast(sellingGeneralAndAdministrativeExpenses as INT64)      as selling_general_and_admin_expenses,
    cast(otherExpenses as INT64)                                as other_expenses,
    cast(costAndExpenses as INT64)                              as cost_and_expenses,
    cast(netInterestIncome as INT64)                            as net_interest_income,
    cast(interestIncome as INT64)                               as interest_income,
    cast(interestExpense as INT64)                              as interest_expense,
    cast(depreciationAndAmortization as INT64)                  as depreciation_and_amortization,
    cast(ebit as INT64)                                         as ebit,
    cast(ebitda as INT64)                                       as ebitda,
    cast(nonOperatingIncomeExcludingInterest as INT64)          as non_operating_income_excluding_interest,
    cast(operatingIncome as INT64)                              as operating_income,
    cast(totalOtherIncomeExpensesNet as INT64)                  as total_other_income_expenses_net,
    cast(incomeBeforeTax as INT64)                              as income_before_tax,
    cast(incomeTaxExpense as INT64)                             as income_tax_expense,
    cast(netIncomeFromContinuingOperations as INT64)            as net_income_from_continuing_operations,
    cast(netIncomeFromDiscontinuedOperations as INT64)          as net_income_from_discontinued_operations,
    cast(otherAdjustmentsToNetIncome as INT64)                  as other_adjustments,
    cast(netIncome as INT64)                                    as net_income,
    cast(eps as FLOAT64)                                        as eps,
    cast(epsdiluted as FLOAT64)                                 as eps_diluted,
    cast(weightedAverageShsOut as INT64)                        as weighted_average_shs_out,
    cast(weightedAverageShsOutDil as INT64)                     as weighted_average_shs_out_diluted,
    safe_divide(
        cast(grossProfit as INT64),
        cast(revenue as INT64)
    )                                                           as gross_margin,
    safe_divide(
        cast(netIncome as INT64),
        cast(revenue as INT64)
    )                                                           as net_margin,
    _ingested_at
from deduped
where _row_num = 1
