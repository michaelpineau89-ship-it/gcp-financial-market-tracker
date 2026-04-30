with source as (
    select * from {{ source('bronze', 'bronze_finnhub_news') }}
),

deduped as (
    select *,
        row_number() over (
            partition by id
            order by _ingested_at desc
        ) as _row_num
    from source
    where id is not null
)

select
    cast(id as STRING)                         as article_id,
    category,
    timestamp_seconds(cast(datetime as INT64)) as published_at,
    headline,
    source,
    summary,
    url,
    image,
    related,
    _ingested_at
from deduped
where _row_num = 1
