with cte as (
select ad_date, url_parameters, coalesce(spend,0) spend, coalesce(impressions,0) impressions, coalesce(reach,0) reach, 
coalesce(clicks,0) clicks, coalesce(leads,0) leads, coalesce(value,0) value
from facebook_ads_basic_daily fabd
union all
select ad_date, url_parameters, coalesce(spend,0) spend, coalesce(impressions,0) impressions, coalesce(reach,0) reach, 
coalesce(clicks,0) clicks, coalesce(leads,0) leads, coalesce(value,0) value
from google_ads_basic_daily gabd
),
	cte2 as (
SELECT date_trunc('month', ad_date) AS ad_month,
case
	when lower(substring(url_parameters, 'utm_campaign=([^\&]+)')) != 'nan' 
	then decode_url_part(lower(substring(url_parameters, 'utm_campaign=([^\&]+)')))
	end as utm_campaign,
sum(spend) as total_spend,
sum(impressions) as total_impressions,
sum(clicks) as total_clicks,
sum(value) as total_value,
case
	when SUM(clicks) > 0 
	then SUM(spend) / SUM(clicks) 
	end AS CPC,
case 
	when SUM(impressions) > 0 
	then SUM(spend) * 1000 / SUM(impressions) 
	end as CPM,
case
	when SUM(impressions) > 0 
	then (SUM(clicks)::float / SUM(impressions) )*100 
	end as CTR,
case
	when SUM(spend) > 0 
    then ((SUM(value::numeric)) / SUM(spend) ) *100
    end as ROMI
FROM cte
group by ad_month, utm_campaign
)
select 
ad_month, 
utm_campaign, 
total_spend, 
total_impressions, 
total_clicks, 
total_value, 
CTR, 
LAG(CTR, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ctr,
			 CTR - LAG(CTR, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS CTR_diff,
CPC, 
LAG(CPC, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_cpc,
			 CPC - LAG(CPC, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS CPC_diff,
CPM, 
LAG(CPM, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
			 CPM - LAG(CPM, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS CPM_diff,
ROMI,
LAG(ROMI, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI,
			 ROMI - LAG(ROMI, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS ROMI_diff
from cte2
order by utm_campaign, ad_month;


