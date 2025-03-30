-- Working with https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=ga4_obfuscated_sample_ecommerce&t=events_20210131&page=table

-- Data preparation for building reports in BI systems

select
  timestamp_micros(event_timestamp) as event_timestamp,
  event_name,
  user_pseudo_id,
  (select value.int_value from e.event_params where key = 'ga_session_id') as session_id,
  geo.country,
  device.category,
  traffic_source.source,
  traffic_source.medium,
  traffic_source.name as campaign
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2021*` e
where event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
limit 1000;


-- Calculation of conversions by dates and traffic channels

with cr_events as (
  select
    timestamp_micros(event_timestamp) as event_timestamp,
    event_name,
    traffic_source.source as source,
    traffic_source.medium as medium,
    traffic_source.name as campaign,
    user_pseudo_id ||
      cast((select value.int_value from e.event_params where key = 'ga_session_id') as string)  as user_session_id
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where event_name in ('add_to_cart', 'begin_checkout', 'purchase', 'session_start')
),

events_count as (
  select
    date(event_timestamp) as event_date,
    source,
    medium,
    campaign,
    count(distinct user_session_id) as user_sessions_count,
    count(distinct case when event_name = 'add_to_cart' then user_session_id end) as added_to_cart_count,
    count(distinct case when event_name = 'begin_checkout' then user_session_id end) as began_checkout_count,
    count(distinct case when event_name = 'purchase' then user_session_id end) as purchase_count,
  from cr_events
  group by 1,2,3,4
)

select
  event_date,
  source,
  medium,
  campaign,
  user_sessions_count,
  added_to_cart_count / user_sessions_count as visit_to_cart,
  began_checkout_count / user_sessions_count as visit_to_checkout,
  purchase_count / user_sessions_count as visit_to_purchase
from events_count
order by 1 desc
limit 1000;


-- Comparison of conversion between different landing pages

with user_sessions as (
  select
    event_name,
    user_pseudo_id ||
      cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string) as user_session_id,
    regexp_extract(
      (select value.string_value from unnest(event_params) where key = 'page_location'),  r'(?:\w+\:\/\/)?[^\/]+\/([^\?#]*)') as page_path,
    (select value.string_value from unnest(event_params) where key = 'page_location') as page_location
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where
  _table_suffix between '20200101' and '20201231'  and event_name = 'session_start'
),

purchases AS (
  SELECT
    user_pseudo_id ||
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS user_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE
    _table_suffix BETWEEN '20200101' AND '20201231'
    AND event_name = 'purchase'
)

select 
  s.page_path,
  count(distinct(s.user_session_id)) as sessions_count,
  count(distinct(p.user_session_id)) as purchase_count,
  count(distinct(p.user_session_id))/count(distinct(s.user_session_id)) as cr_to_purchase
from user_sessions s
left join purchases p using(user_session_id)
group by 1
order by 2 desc
limit 10;



-- Checking the correlation between user engagement and purchases

with sessions as (
  SELECT
    user_pseudo_id,
    CAST(
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING
    ) AS session_id,
    cast(SUM(CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' THEN 1 ELSE 0 END) AS INT64) AS is_engaged_count,
    SUM(
      COALESCE(
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS INT64),
        0
      )
    ) AS total_activity_time
    
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e

  GROUP BY
    user_pseudo_id,
    session_id
),
purchases AS (
  
  SELECT
    user_pseudo_id,
    CAST(
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING
    ) AS session_id,
    CAST(MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS INT64) AS made_purchase
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  
  GROUP BY
    user_pseudo_id,
    session_id
)
SELECT

  CORR(is_engaged_count, made_purchase) AS correlation_engagement_purchase,
  CORR(total_activity_time, made_purchase) AS correlation_activity_time_purchase
FROM
  sessions s
LEFT JOIN
  purchases p
USING (user_pseudo_id, session_id)

limit 100;

