/*Filter by the first day of the month of the date the ad was shown.
For each utm_campaign in each month, add a new field: 'difference in CPM, CTR and ROMI' in the current month compared to the previous one in percentage.*/

WITH cte AS (
SELECT
	ad_date,
	url_parameters,
	COALESCE(spend, 0) spend,
	COALESCE(impressions, 0) impressions,
	COALESCE(reach, 0) reach,
	COALESCE(clicks, 0) clicks,
	COALESCE(leads, 0) leads,
	COALESCE(value, 0) value
FROM
	facebook_ads_basic_daily fabd
UNION ALL
SELECT
	ad_date,
	url_parameters,
	COALESCE(spend, 0) spend,
	COALESCE(impressions, 0) impressions,
	COALESCE(reach, 0) reach,
	COALESCE(clicks, 0) clicks,
	COALESCE(leads, 0) leads,
	COALESCE(value, 0) value
FROM
	google_ads_basic_daily gabd
),
	cte2 AS (
SELECT
	date_trunc('month', ad_date) AS ad_month,
	CASE
		WHEN LOWER(SUBSTRING(url_parameters, 'utm_campaign=([^\&]+)')) != 'nan' 
		THEN decode_url_part(LOWER(SUBSTRING(url_parameters, 'utm_campaign=([^\&]+)')))
		END AS utm_campaign,
	SUM(spend) AS total_spend,
	SUM(impressions) AS total_impressions,
	SUM(clicks) AS total_clicks,
	SUM(value) AS total_value,
	CASE
		WHEN SUM(clicks) > 0 
		THEN SUM(spend) / SUM(clicks)
		END AS CPC,
	CASE
		WHEN SUM(impressions) > 0 
		THEN SUM(spend) * 1000 / SUM(impressions)
		END AS CPM,
	CASE
		WHEN SUM(impressions) > 0 
		THEN (SUM(clicks)::float / SUM(impressions) )* 100
		END AS CTR,
	CASE
		WHEN SUM(spend) > 0 
    	THEN ((SUM(value::NUMERIC)) / SUM(spend) ) * 100
		END AS ROMI
FROM
	cte
GROUP BY
	ad_month,
	utm_campaign
)
SELECT
	ad_month,
	utm_campaign,
	total_spend,
	total_impressions,
	total_clicks,
	total_value,
	CTR,
	LAG(CTR, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ctr,
	CTR - LAG(CTR, 1) OVER (PARTITION BY utm_campaign ORDER BY	ad_month) AS CTR_diff,
	CPC,
	LAG(CPC, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_cpc,
	CPC - LAG(CPC, 1) OVER (PARTITION BY utm_campaign ORDER BY	ad_month) AS CPC_diff,
	CPM,
	LAG(CPM, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_CPM,
	CPM - LAG(CPM, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS CPM_diff,
	ROMI,
	LAG(ROMI, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS prev_ROMI,
	ROMI - LAG(ROMI, 1) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS ROMI_diff
FROM
	cte2
ORDER BY
	utm_campaign,
	ad_month;
