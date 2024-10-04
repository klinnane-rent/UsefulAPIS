-- Snowflake-SQL template for SRP-centric experiment (cd80-cd81 clauses must be manually replaced due to overlapping risk)
-- template vars: {brand}, {brand_stream_id}, {start_date}, {end_date}
--
with variant_list as
(SELECT DISTINCT
    user_pseudo_id userid
        ,CASE WHEN event_params:cd81 LIKE '%listing_card_redesign_version_1%' THEN 'Control'
         WHEN event_params:cd81 LIKE '%listing_card_redesign_version_2%' THEN 'Variant' end as Variant
    FROM EDW.WEB_ACTIVITY.GA4_EVENT
    WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
           AND event_params:cd80 LIKE '%ag_listing_card_redesign_ab_test%')

,no_multiples AS
  (SELECT
    userid
    , count(DISTINCT Variant) nVariant
  FROM variant_list
  GROUP BY 1
  HAVING nVariant = 1)

,session_check AS
(SELECT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid
        ,user_pseudo_id as userid
        ,CASE WHEN event_params:cd81 LIKE '%listing_card_redesign_version_1%' THEN 'Control'
         WHEN event_params:cd81 LIKE '%listing_card_redesign_version_2%' THEN 'Variant' end as Variant
        , min(event_timestamp) as first_hit  -- map to event_timestamp
    FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
INNER JOIN no_multiples
    ON evt.user_pseudo_id = no_multiples.userid

    WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    --AND event_name='page_view'  -- should also restrict to 'SRP': event_params:cd10 in ('srp', 'srp_map') ???
    AND (event_name='page_view' and event_params:cd10 LIKE '%srp%')
    AND event_params:cd80 LIKE '%ag_listing_card_redesign_ab_test%'
    GROUP BY 1,2,3)

,session_info AS
(SELECT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid
        ,user_pseudo_id userid
        --,'unknown' channelGrouping  -- place-holder
        ,device_category devicecategory
        --, CASE WHEN sessions.totals.bounces >= 1 THEN 1 ELSE 0 END as bounces
        --,1 - max(CASE WHEN event_params:session_engaged_event>0 THEN 1 ELSE 0 END) as bounces
        ,case when max(cast(event_params:engaged_session_event as integer))>=1 then 0 else 1 end as bounces

        --,0 bounces  -- place-holder
        --,(1 - max(cast(event_params:session_engaged as integer))) as bounces  -- XXX not working, use 'engaged_session_event' instead
        ,max(TO_DATE(cast(event_date as string), 'YYYYMMDD')) as date
        ,(listagg(event_params:cd10, ',') WITHIN GROUP (order by event_timestamp)) as test_renter_journey
    FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
    LEFT JOIN session_check ON session_check.sessionid = concat(user_pseudo_id, '_', event_params:ga_session_id)
    WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    and    session_check.sessionid IS NOT NULL
    AND event_name='page_view'
    --group by 1,2,3,4)
    group by 1,2,3)

-- location_category is fixed with city-state?
,SRP_Category_counts AS
(SELECT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid
    ,COUNT(DISTINCT concat(event_params:cd11, event_params:cd12, event_params:cd138)) as categories
    FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
    WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    and event_params:cd10 LIKE '%srp%'
    and event_params:cd13 = 'city'
    and event_params:cd138 !=  'undefined'
    and event_params:cd138 IS NOT NULL
    GROUP BY 1)
,SRP_Category AS
( -- union open
(SELECT DISTINCT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid
  , cast(event_params:cd138 as string) as location_category
  , concat(event_params:cd11, ', ', event_params:cd12) as City
FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
LEFT JOIN SRP_Category_counts
ON SRP_Category_counts.sessionid = concat(user_pseudo_id, '_', event_params:ga_session_id)
WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    and event_params:cd10 LIKE '%srp%'
    and event_params:cd138 !=  'undefined'
    and event_params:cd138 IS NOT NULL
    and event_params:cd13 = 'city'  -- XXX is this needed, given that 99+% srps are 'city'
    and SRP_Category_counts.categories = 1
)
UNION ALL
(SELECT DISTINCT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid
  ,'Multiple Location Categories' as location_category
  ,'Multiple Cities' as City
FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
LEFT JOIN SRP_Category_counts
ON SRP_Category_counts.sessionid = concat(user_pseudo_id, '_', event_params:ga_session_id)
WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    and event_params:cd10 LIKE '%srp%'
    and event_params:cd138 !=  'undefined'
    and event_params:cd138 IS NOT NULL
    and event_params:cd13 = 'city'  -- XXX is this needed, given that 99+% srps are 'city'
    and SRP_Category_counts.categories > 1  -- only difference from the previous block
)
) -- union close

,lead_info as
(SELECT DISTINCT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid

,COUNT(DISTINCT CASE WHEN event_params:cd67 IN ('phone', 'email') THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as total_leads
,COUNT(DISTINCT CASE WHEN event_params:cd67 IN ('phone', 'email') and event_params:cd10 LIKE '%srp%' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as total_leads_srp
,COUNT(DISTINCT CASE WHEN lower(event_params:cd28) in ('myads','aptguide') and event_params:cd67 IN ('phone', 'email') and event_params:cd15 = 'paid' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as core_total_leads
,listagg((case when lower(event_params:cd28) IN ('myads','aptguide') and event_params:cd67 IN ('phone', 'email') and  event_params:cd15='paid' then event_params:cd09 end), ',') as core_lids

,count(distinct case when lower(event_params:cd28) IN ('myads','aptguide') and event_params:cd15='paid' and event_params:cd130='phone' then event_params:cd09 end) as LT_phone
,count(distinct case when lower(event_params:cd28) IN ('myads','aptguide') and event_params:cd15='paid' and event_params:cd130='email_no_intent' then event_params:cd09 end) as LT_email_no_intent
,count(distinct case when lower(event_params:cd28) IN ('myads','aptguide') and event_params:cd15='paid' and event_params:cd130='email_intent' then event_params:cd09 end) as LT_email_intent
,count(distinct case when lower(event_params:cd28) IN ('myads','aptguide') and event_params:cd15='paid' and event_params:cd130='request_tour' then event_params:cd09 end) as LT_request_tour
,count(distinct case when lower(event_params:cd28) IN ('myads','aptguide') and event_params:cd15='paid' and event_params:cd130='schedule_tour' then event_params:cd09 end) as LT_schedule_tour

FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
LEFT JOIN session_check on session_check.sessionid = concat(user_pseudo_id, '_', event_params:ga_session_id)
LEFT JOIN SRP_Category on SRP_Category.sessionid = concat(user_pseudo_id, '_', event_params:ga_session_id)

WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    and event_name='lead_submission'
    AND session_check.sessionid IS NOT NULL
    AND event_timestamp >= session_check.first_hit
 group by 1)

,prop_interactions AS
(SELECT DISTINCT concat(user_pseudo_id, '_', event_params:ga_session_id) as sessionid
-- not use "distinct" for engagement measuring
,COUNT(CASE WHEN lower(event_params:cd10) LIKE '%srp%' and event_name='custom_click' and event_params:click_sub_type = 'pinpoint' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as map_pin_click
,COUNT(CASE WHEN lower(event_params:cd10) LIKE '%srp%' and event_name='custom_click' and event_params:click_sub_type = 'favorite_properties' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as srp_saves   -- "cd41: save"; "cd41: unsave"
,COUNT(CASE WHEN lower(event_params:cd10) LIKE '%srp%' and event_name='custom_click' and event_params:click_sub_type = 'share_button' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as srp_shares   -- "cd41: save"; "cd41: unsave"
,COUNT(CASE WHEN lower(event_params:cd10) LIKE '%srp%' and event_name='custom_click' and event_params:click_type LIKE '%photo%gallery%' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as srp_gallery
,COUNT(CASE WHEN lower(event_params:cd10) LIKE '%srp%' and event_name='custom_click' and event_params:click_sub_type LIKE '%create%saved%search%' THEN concat(event_params:cd09, user_pseudo_id) ELSE null END) as saved_search


FROM EDW.WEB_ACTIVITY.GA4_EVENT evt
LEFT JOIN session_check on session_check.sessionid = concat(user_pseudo_id, '_', event_params:ga_session_id)
WHERE stream_id={brand_stream_id} and (event_date between '{start_date}' and '{end_date}')
    AND session_check.sessionid IS NOT NULL
    AND event_timestamp >= session_check.first_hit
 group by 1)

,session_results as (
SELECT session_check.sessionid
, session_check.userid
, session_check.variant

, SRP_Category.location_category

, session_info.date
--, session_info.channelGrouping Channel
, session_info.devicecategory Device
, session_info.bounces
--, session_info.engaged
--, session_info.test_renter_journey
, (length(test_renter_journey) - LENGTH(REGEXP_REPLACE(session_info.test_renter_journey, 'srp,pdp', '')))/7 as srp_pdp
, (length(test_renter_journey) - LENGTH(REGEXP_REPLACE(session_info.test_renter_journey, 'srp', '')))/3 as srp_views

, lead_info.total_leads
, lead_info.total_leads_srp
, lead_info.core_total_leads
, lead_info.core_lids
,lead_info.LT_phone
,lead_info.LT_email_no_intent
,lead_info.LT_email_intent
,lead_info.LT_request_tour
,lead_info.LT_schedule_tour

, prop_interactions.map_pin_click
, prop_interactions.srp_saves
, prop_interactions.srp_shares
, prop_interactions.srp_gallery
, prop_interactions.saved_search as srp_saved_search
FROM session_check
LEFT JOIN session_info
    ON session_check.sessionid = session_info.sessionid
LEFT JOIN SRP_Category
     ON session_check.sessionid = SRP_Category.sessionid
LEFT JOIN lead_info
    ON session_check.sessionid = lead_info.sessionid
LEFT JOIN prop_interactions
    ON session_check.sessionid = prop_interactions.sessionid
ORDER BY 2
)
,session_results2 as (
--Engagement = (SRP-to-PDP Views + Map Pin Interactions + SRP Leads + Favorites + Photo Gallery Clicks) / (SRP Views)
select *
,'{brand}' as Brand
,coalesce(srp_pdp,0)+coalesce(map_pin_click,0)+coalesce(total_leads_srp,0)+coalesce(srp_saves,0)+coalesce(srp_shares,0)+coalesce(srp_gallery,0) + coalesce(saved_search,0) as srp_engagements
from session_results
)

--SELECT
--    date
--    , 'AG' as Brand
--    ,variant
--    ,Channel
--    ,Device
--    , location_category
--
--    , COUNT(DISTINCT sessionid) as sessions
--    , sum(bounces) bounces -- bounce rate
--
--    , sum(srp_views) srp_views -- ctr
--    , sum(srp_pdp) srp_pdp
--
--    , sum(total_leads) total_leads  -- conversions
--    , sum(Core_total_leads) Core_total_leads
--
--    , sum(total_leads_srp) total_leads_srp -- engagement score
--    , sum(map_pin_click) map_pin_click
--    , sum(srp_saves) AS srp_saves
--    , sum(srp_gallery) AS srp_gallery
--    , sum(srp_engagements) as srp_engagements
--    , sum(saved_search) as srp_saved_search
--FROM session_results
--GROUP BY 1,2,3,4,5,6

--select * from session_check
--select * from session_info
--select * from SRP_Category
--select * from lead_info
--select * from prop_interactions
select * from session_results2
