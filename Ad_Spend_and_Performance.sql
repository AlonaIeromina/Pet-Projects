/*Task Summary: Aggregating Advertising Platform Data in SQL
Objective:
Combine data from Facebook Ads and Google Ads to create a unified report of marketing campaign performance.
Steps:
Merge Facebook Ads Data:
	Join facebook_ads_basic_daily, facebook_adset, and facebook_campaign.
	Extract key fields
Merge Google Ads Data:
	Combine the first CTE with google_ads_basic_daily.
	Ensure all relevant fields from both sources are included.
Create a Final Aggregated Table:
	Select key columns
	Aggregate metrics by ad_date, media_source, campaign_name, adset_name
Expected Outcome:
A single dataset summarizing Facebook & Google Ads performance per campaign and ad set per day.*/

-- Step 1: First CTE to combine data from facebook_ads_basic_daily, facebook_adset, and facebook_campaign
WITH facebook_data AS (
SELECT
	fabd.ad_date,
	fc.campaign_name,
	fa.adset_name,
	fabd.spend,
	fabd.impressions,
	fabd.reach,
	fabd.clicks,
	fabd.leads,
	fabd.value
FROM
	facebook_ads_basic_daily fabd
JOIN facebook_adset fa
        ON
	fabd.adset_id = fa.adset_id
JOIN facebook_campaign fc
        ON
	fabd.campaign_id = fc.campaign_id
),
-- Step 2: Second CTE to combine facebook_data and google_ads_basic_daily
total_data AS (
SELECT
	fd.ad_date,
	'Facebook Ads' AS media_source,
	fd.campaign_name,
	fd.adset_name,
	fd.spend,
	fd.impressions,
	fd.clicks,
	fd.value
FROM
	facebook_data fd
UNION ALL
SELECT
	gabd.ad_date,
	'Google Ads' AS media_source,
	gabd.campaign_name,
	gabd.adset_name,
	gabd.spend,
	gabd.impressions,
	gabd.clicks,
	gabd.value
FROM
	google_ads_basic_daily gabd
)
-- Step 3: Aggregating combined data
SELECT
	ad_date,
	media_source,
	campaign_name,
	adset_name,
	SUM(spend) AS total_spend,
	SUM(impressions) AS total_impressions,
	SUM(clicks) AS total_clicks,
	SUM(value) AS total_value
FROM
	total_data
GROUP BY
	ad_date,
	media_source,
	campaign_name,
	adset_name;
