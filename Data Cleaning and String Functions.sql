CREATE OR REPLACE FUNCTION urldecode(p varchar) RETURNS varchar AS $$
SELECT convert_from( CAST(E'\\x' || string_agg( CASE WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'SQL_ASCII'), 'hex')
                                                     ELSE substring(r.m[1] from 2 for 2) END, '') AS bytea), 'UTF8')
FROM regexp_matches(replace($1, '+', ' '), '%[0-9a-f][0-9a-f]|.', 'gi') AS r(m);
$$ LANGUAGE SQL STRICT;
WITH adds_cte AS (SELECT ad_date, url_parameters,
        COALESCE (spend, 0) as spend,
        COALESCE (impressions, 0) as impressions,
        COALESCE (reach, 0) as reach,
        COALESCE (clicks, 0) as clicks,
        COALESCE (leads, 0) as leads,
        COALESCE (value, 0) as value
        FROM facebook_ads_basic_daily
        UNION ALL
        SELECT ad_date, url_parameters,
        COALESCE (spend, 0) as spend,
        COALESCE (impressions, 0) as impressions,
        COALESCE (reach, 0) as reach,
        COALESCE (clicks, 0) as clicks,
        COALESCE (leads, 0) as leads,
        COALESCE (value, 0) as value
        FROM google_ads_basic_daily gabd )
SELECT
    ad_date,
    CASE
        WHEN LOWER(SUBSTRING(url_parameters,'utm_campaign=([^&#$]+)')) = 'nan' THEN NULL
        ELSE urldecode(LOWER(SUBSTRING(url_parameters,'utm_campaign=([^&#$]+)')))
    END AS utm_campaign,
    SUM(spend) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(value) AS total_value,
    CASE WHEN SUM (impressions)>0 THEN ROUND((SUM(clicks)::NUMERIC/SUM(impressions))*100,2) END AS ctr,
    CASE WHEN SUM (clicks)>0 THEN ROUND((SUM(spend)::NUMERIC/SUM(clicks)), 2) END AS cpc,
    CASE WHEN SUM (impressions)>0 THEN ROUND(((SUM(spend)::NUMERIC*1000)/SUM(impressions)), 2) END AS cpm,
    CASE WHEN SUM (spend)>0 THEN ROUND((SUM(value)::NUMERIC/SUM(spend))*100,2) END AS romi
FROM adds_cte
GROUP BY 1,2
ORDER BY 1,2;