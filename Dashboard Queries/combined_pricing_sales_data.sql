/*
CONTEXT:
 - One query for all of WeWork's sales-related Global Pricing Data

 RESULT EXPECTATION:
 - This query returns sales transactions that resulted in at least 1 gross desk sale at the reservation level, starting from 2019-06-01

 ASSUMPTION:
 - Each transaction should have a unique pairing of Deal_UUID and Primary_Reservation_UUID, so we do not expect any duplicate rows
 - There are a list of deals that have been excluded due to contract upload errors
 - Data is collected at the time of sale, and any cancellations or amendments to a sale that happen in a subsequent month result in a new row of data
 */

with deal_exclusions as (
    --excludes list of deal_uuids that get updated in a Google Sheet from query output
    select * from fivetran.google_sheets.pricing_dash_exclusions
)

,discount_reason as (
    --pulls reservations and commentary on discounts or explanation on "Out of Policy" status of reservation in r.notes field
    select distinct 
        r.uuid
        ,r.notes
    from  fivetran.spaceman_public.reservations r
)

,brazil_adj as (
    --adjusting Brazil pricing to include VAT
    select distinct
        sales_reporting_month
        ,territory
        ,deal_uuid
        ,primary_reservation_uuid
        ,vat_adjust_factor
    from central.cdm_sales.sales_records
    where territory = 'Brazil'
)

,occupancy1 as (
    --pulls Next 3 Months Core: Building Occupancy
    select distinct
        location_uuid
        , location_name
        , sum(a.occupancy) as n3m_AddOccup
        , sum(a.capacity) as n3m_AddCap
        , sum(a.occupancy)/sum(a.capacity) as n3m_Occupancy
    from central.cdm.space_inventory_bom a
    left join central.cdm.accounts b on a.account_uuid = b.account_uuid
    where sku <> 'HD'
        and (is_wework_inc_affiliates = 'FALSE' or is_wework_inc_affiliates is null)
        and date >= date_trunc('month',dateadd('month',1,current_date)) and date < date_trunc('month',dateadd('month',4,current_date))
    group by a.location_uuid,2
)

,occupancy_current as (
    --pulls Monthly Core: Building Occupancy
    select distinct
        a.report_month
        ,a.location_uuid
        ,sum(a.occupancy) as occ_current
        ,sum(a.capacity) as cap_current
        ,sum(a.occupancy)/sum(a.capacity) as current_building_occupancy
    from central.cdm.space_inventory_bom a
    left join central.cdm.locations l on a.location_uuid = l.uuid
    left join central.cdm.accounts b on a.account_uuid = b.account_uuid
    where a.sku <> 'HD'
        and (is_wework_inc_affiliates = 'FALSE' or is_wework_inc_affiliates is null)
        and report_month >= '2019-06-01'
        and region not in ('China','India')
    group by 1,2
)
    
,sku_occ as (
    --pulls Next 3 Months Core: Building SKU Occupancy (SKUs are 1P, 2P, 3P, 4P, 5-6P, 7-10P, 11-14P, 15-20P, 21-30P, 31-50P, 51-100P, 101-200P, 201-500P, 500P+)
    select distinct
        location_uuid
        , location_name
        , a.sku
        , sum(a.occupancy) as SKU_occ
        , sum(a.capacity) as SKU_capacity
        , sum(a.occupancy)/sum(a.capacity) as SKU_Occupancy
    from central.cdm.space_inventory_bom a
    left join central.cdm.accounts b on a.account_uuid = b.account_uuid
    where a.sku <> 'HD'
        and (is_wework_inc_affiliates = 'FALSE' or is_wework_inc_affiliates is null)
        and date >= date_trunc('month',dateadd('month',1,current_date)) and date < date_trunc('month',dateadd('month',4,current_date))
    group by a.location_uuid, location_name,a.sku
)

,emails as (
    --pulls sales rep emails associated with each Salesforce Opportunity ID
    select distinct
        sf_opportunity_id
        , listagg(distinct u1.email, ', ') WITHIN GROUP(ORDER BY u1.email) as email_a
        , listagg(distinct u2.email, ', ') WITHIN GROUP(ORDER BY u2.email) as email_b
    from growth.growthtech_dw.dim_sf_opportunity o
    left join CENTRAL.TRANSFORMATION_OFFICE.SALES_COMP_MONTHLY_TEAM_MEMBER t
        on o.sf_opportunity_id = t.team_sf_object_id
    left join growth.growthtech_dw.dim_sf_user u1
        on t.sf_user_id = u1.sf_user_id
    left join growth.growthtech_dw.dim_sf_user u2
        on o.opportunity_owner_id = u2.sf_user_id
    group by 1
)

,deals_and_emails as (
    --combines sales rep email information with sales data for easier connection to main table
    select distinct
        a.sales_reporting_month,
        a.deal_uuid,
        a.primary_reservation_uuid,
        a.salesforce_opportunity_id,
        CONCAT(coalesce(email_a,email_b) || ', ' || case when email_a is not null then email_b else NULL END) as email_list
    from central.cdm_sales.primary_reservation_deal_summary a
    left join emails e
        on a.salesforce_opportunity_id = e.sf_opportunity_id
    where a.sales_reporting_month >= '2022-01-01'
        -- and a.gross_desk_sales > 0
    order by a.salesforce_opportunity_id
)

,promo_codes as (
    --pulls promo codes selected in Spacestation associated with most recent term_id in a member's reservation
    select distinct
        a.reservation_uuid
        , a.location_name
        , a.account_name
        , a.term_id
        , a.commitment_term_created_utc
        , a.commitment_term_start_utc
        , a.commitment_term_end_utc
        , max(coalesce(applied_discount,NULL)) OVER(PARTITION BY a.reservation_uuid, a.term_id ORDER BY a.commitment_term_created_utc) as applied_discount
        ,case when a.commitment_term_created_utc is not NULL then rank() OVER(partition by case when a.commitment_term_created_utc is not NULL then a.reservation_uuid else NULL end,a.term_id ORDER BY a.commitment_term_created_utc desc) else NULL END as rank
    from central.cdm.space_inventory_bom a
    left join central.cdm.accounts b 
        on a.account_uuid = b.account_uuid
    where sku <> 'HD'
        and (is_wework_inc_affiliates = 'FALSE' or is_wework_inc_affiliates is null)
        and a.commitment_term_cancelled_utc is NULL
        and a.term_id is not NULL
        and a.term_id <> '422971'
)


,disc AS (
    --pulls the monthly rate (MRR) for each reservation and term to determine the value of a "free month"
    SELECT  sales_reporting_month
       ,deal_uuid
       ,primary_reservation_uuid
       ,primary_reservation_capacity
       ,gross_committed_value_changed_before_discount_local
       ,addon_committed_value_changed_local
       ,resulting_commitment_term_months
       ,ROUND((gross_committed_value_changed_before_discount_local - addon_committed_value_changed_local)/NULLIFZERO(resulting_commitment_term_months),0) AS MRR
    FROM central.cdm_sales.primary_reservation_deal_summary
    WHERE COALESCE(is_wework_inc_affiliates,FALSE)=FALSE
        AND subregion NOT IN ('China','India','Israel')
        AND product NOT ILIKE '%WeWork All Access%'
        AND (gross_desk_sales > 0 OR resulting_commitment_term_months>0)
)

,free_base AS (
    -- If discount value is = MRR then it is a free month deduction
    SELECT  s.sales_reporting_month
       ,s.primary_reservation_uuid
       ,s.deal_uuid
       ,s.location_uuid
       ,s.price_id
       ,s.applied_months
       ,s.price_local
       ,s.price_usd_sales_comp_fx
       ,s.committed_value_changed_local
       ,d.MRR
       ,s.primary_reservation_capacity * s.applied_months AS  disc_desk_months
       ,CASE WHEN d.primary_reservation_uuid IS NOT NULL THEN TRUE ELSE FALSE END AS is_free_month
    FROM central.cdm_sales.sales_records s
    LEFT JOIN disc d
        ON d.sales_reporting_month=s.sales_reporting_month
        AND d.deal_uuid=s.deal_uuid
        AND d.primary_reservation_uuid=s.primary_reservation_uuid
        AND ABS(ROUND(s.price_local,0))=d.mrr -- discount price is expressed as monthly value
    WHERE 1=1
        AND s.sales_record_reservation_type='DiscountReservation'
        AND s.applied_months > 0 -- only incl. "create discount" record types
)

,free_m AS (
    --aggregates the number of free months and desk months associated with each deal and reservation
    SELECT  
        sales_reporting_month
       ,primary_reservation_uuid
       ,deal_uuid
       ,MIN(MRR) AS MRR
       ,SUM(applied_months) AS applied_months
       ,SUM(disc_desk_months) AS disc_desk_months
    FROM free_base
    WHERE is_free_month
    GROUP BY 1,2,3
)

-- The below queries are used as an additional data source to collect Promo Code usage. Each CTE is created for a specific promo and searches through the r.notes field for phrases similar to the ones identified in the WHERE clause
,YEAROFTHERABBIT as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as yearoftherabbit_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%YEAROFTHERABBIT%' or r.notes like '%rabbit%' or r.notes like '%yearoftherabbit%' or r.notes like '%RABBIT%' 
    group by r.created_at,r.updated_at,r.uuid,r.notes
)

,BIFCUSPACE as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as bifcuspace_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%bifcuspace%' or r.notes like '%space%bifcu%' or r.notes like '%BIFCUSPACE%' 
    group by r.created_at,r.updated_at,r.uuid,r.notes
)

,ssq_badapple as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as ssqbadapple_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%SSQBADAPPLE%' or r.notes like '%ssqbadapple%' /*or r.notes like '%badapple%'*/ /*removing this one because it conflicts with the badapple tracking further down */ or r.notes like '%badapplessq%' or r.notes like '%ssq%bad%apple%' 
    group by r.created_at,r.updated_at,r.uuid,r.notes
    )
    
,offkr_250 as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as offkr250_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%250OFFKR%' or r.notes like '%250%OFFKR%' or r.notes like '%250offkr%' or r.notes like '%250%offkr%'
    group by r.created_at,r.updated_at,r.uuid,r.notes
    )
,offkr_350 as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as offkr350_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%350OFFKR%' or r.notes like '%350%OFFKR%' or r.notes like '%350offkr%' or r.notes like '%350%offkr%'
    group by r.created_at,r.updated_at,r.uuid,r.notes
    )
,jakarta_23 as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as jakarta23_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%JAKARTA23%' or r.notes like '%jakarta%23%' or r.notes like '%JAKARTA%23%' 
    group by r.created_at,r.updated_at,r.uuid,r.notes
    )

,grow_with_us as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as growwithus_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%GROW%WITH%US%' or r.notes like '%GROWWITHUS%' or r.notes like '%growwithus%' or r.notes like '%GrowWithUs%' or r.notes like '%Grow%with%us%' or r.notes ilike '%grow%with%us%' or r.notes ilike '%GWU%' or r.notes like '%Grow%with%US%' or r.notes like '%Grow%With%Us%'
    group by r.created_at,r.updated_at,r.uuid,r.notes
)
        
,q3_space as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as q3space_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%Q3SPACEPROMO%' or r.notes like '%Q3%SPACE%PROMO%' or r.notes like '%Q3SpacePromo%' or r.notes like '%q3%space%promo%' or r.notes like '%Q3%Space%Promo%' or r.notes like '%Q3%space%promo%' or r.notes like '%Q3%Dedicated%space%promo%' or r.notes like '%Q3%PROMO%' or r.notes like '%q3%promo%' or r.notes like '%Q3%Promo%' or r.notes like '%Q3%promo%' or r.notes like '%Q3SpacePRomo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,growth_campus as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as growthcampus_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%GROWTHCAMPUS%' or r.notes ilike '%GROWTH%CAMPUS%' or r.notes like '%Growth%Campus%' or r.notes like '%growth%campus%' or r.notes like '%GrowthCampus%' or r.notes like '%growthcampus%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,usc_small_upgrades as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as uscsmall_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%USCSMALLUPGRADES%' or r.notes like '%usc%small%upgrades%' or r.notes like '%USC%Small%Upgrades%' or r.notes like '%uscsmallupgrades%' 
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,au_q4 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as auq4_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%AUQ4PROMO%' or r.notes like '%auq4promo%' or r.notes like '%AU%Q4%Promo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,q4_space as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as q4space_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%Q4SPACEPROMO%' or r.notes ilike '%Q4%SPACEPROMO%'or r.notes like '%Q4%SPACE%PROMO%' or r.notes like '%Q4SpacePromo%' or r.notes like '%q4%space%promo%' or r.notes like '%Q4%Space%Promo%' or r.notes like '%Q4%space%promo%' or r.notes like '%Q4%Dedicated%space%promo%' or r.notes like '%Q4%PROMO%' or r.notes like '%q4%promo%' or r.notes like '%Q4%Promo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,q4_renewal as (
    select distinct 
        r.created_at, r.updated_at, r.uuid, r.notes, count(*) as q4renewal_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%Q4RENEWAL%' or r.notes ilike '%Q4%RENEWAL%'or r.notes like '%Q4%renewal%' or r.notes like '%q4%Renewal%' or r.notes like '%Q4%Renewal%' or r.notes like '%q4%renewal%' or r.notes like '%q4renewal%' or r.notes like '%Q4Renewal%'
        group by r.created_at, r.updated_at, r.uuid,r.notes
)

,q1_tourpromo as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as q1tour_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%Q1TOURPROMO%' or r.notes ilike '%Q1%TOURPROMO%'or r.notes like '%Q1%tourpromo%' or r.notes like '%q1%Tour%Promo%' or r.notes like '%Q1%tour%promo%' or r.notes like '%q1%tour%promo%' or r.notes like '%q1tourpromo%' or r.notes like '%Q1TourPromo%' or r.notes like '%Q1%TOUR%PROMO%'
        or r.notes like '%Q1%Tour%Promo%' or r.notes like '%Q2%Tour%Promo%' or r.notes like '%Tour%Promo%' or r.notes like '%Q2%TOUR%PROMO%' or r.notes like '%Q2%Tour%promo%' or r.notes like '%Q2Tourpromo%' or r.notes like '%Tour%promo%' or r.notes like '%Tourpromo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,segrowthcampus as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as segrowth_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%SEGROWTHCAMPUS%' or r.notes ilike '%SE%GROWTHCAMPUS%'or r.notes like '%SE%GROWTH%CAMPUS%' or r.notes like '%segrowthcampus%' or r.notes like '%se%growthcampus%' or r.notes like '%se%growth%campus%' or r.notes like '%SE%GrowthCampus%' or r.notes like '%SE%Growth%Campus%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,neceegrowthcampus as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as neceegrowth_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%NE&CEEGROWTHCAMPUS%' or r.notes ilike '%NE%&%CEE%GROWTHCAMPUS%'or r.notes like '%NE%&%CEE%GROWTH%CAMPUS%' or r.notes like '%ne&ceegrowthcampus%' or r.notes like '%ne&cee%growthcampus%' or r.notes like '%ne%&%cee%growth%campus%' or r.notes like '%NE%&%CEE%GrowthCampus%' or r.notes like '%NE%&%CEE%Growth%Campus%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,ukemgrowthcampus as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as ukemgrowth_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%UKEMGROWTHCAMPUS%' or r.notes ilike '%UKEM%GROWTHCAMPUS%'or r.notes like '%UK%EM%GROWTH%CAMPUS%' or r.notes like '%ukemgrowthcampus%' or r.notes like '%ukem%growthcampus%' or r.notes like '%uk%em%growth%campus%' or r.notes like '%UKEM%GrowthCampus%' or r.notes like '%UK%EM%Growth%Campus%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,seagrowthcampus as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as seagrowth_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%SEAGROWTHCAMPUS%' or r.notes ilike '%SEA%GROWTHCAMPUS%'or r.notes like '%seagrowthcampus%' or r.notes like '%SEA%growthcampus%' or r.notes like '%sea%growth%campus%' or r.notes like '%SEA%GrowthCampus%' or r.notes like '%sea%Growth%Campus%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,anzgrowthcampus as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as anzgrowth_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%ANZGROWTHCAMPUS%' or r.notes ilike '%ANZ%GROWTHCAMPUS%'or r.notes like '%anzgrowthcampus%' or r.notes like '%ANZ%growthcampus%' or r.notes like '%anz%growth%campus%' or r.notes like '%ANZ%GrowthCampus%' or r.notes like '%anz%Growth%Campus%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,yearofthetiger as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as tiger_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%YEAROFTHETIGER%' or r.notes ilike '%YEAR%OF%THE%TIGER%'or r.notes like '%year%of%the%tiger%' or r.notes like '%Tiger%' or r.notes like '%tiger%' or r.notes like '%TIGER%' or r.notes like '%Year%of%the%Tiger%' or r.notes like '%Year%Of%The%Tiger%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,ukraine50 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as ukraine50_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%UKRAINE50%' or r.notes ilike '%UKRAINE%50%'or r.notes like '%Ukraine50%' or r.notes like '%Ukraine%50%' or r.notes like '%ukraine50%' or r.notes like '%ukraine%50%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,ukraine100 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as ukraine100_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%UKRAINE100%' or r.notes ilike '%UKRAINE%100%'or r.notes like '%Ukraine100%' or r.notes like '%Ukraine%100%' or r.notes like '%ukraine100%' or r.notes like '%ukraine%100%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,bundle2022 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as bundle2022_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%BUNDLE2022%' or r.notes ilike '%BUNDLE%2022%'or r.notes like '%Bundle2022%' or r.notes like '%Bundle%2022%' or r.notes like '%bundle2022%' or r.notes like '%bundle%2022%' or r.notes like '%bundle%promo%' or r.notes like '%bundlepromo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,summerspace as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as summerspace_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%SUMMERSPACEPROMO%' or r.notes ilike '%SUMMER%SPACE%PROMO%'or r.notes like '%SummerSpacePromo%' or r.notes like '%Summer%Space%Promo%' or r.notes like '%summerspacepromo%' or r.notes like '%summer%space%promo%' or r.notes like '%Summerspacepromo%' or r.notes like '%Summer%space%promo%' or r.notes like '%SUMMERSPACE%' or r.notes like '%sUMMERspACE%PROMO%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,flex2020 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as flex2020_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%FLEX2020%' or r.notes like '%FLEX%2020%' or r.notes like '%flex2020%' or r.notes like '%flex%2020%' or r.notes like '%Flex2020%' or r.notes like '%Flex%2020%' or r.notes like '%FLex2020%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,flex2021 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as flex2021_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%FLEX2021_PACIFIC%' or r.notes like '%FLEX2021%' or r.notes like '%FLEX%2021%' or r.notes like '%flex2021%' or r.notes like '%flex%2021%' or r.notes like '%flex2021_pacfic%' or r.notes like '%Flex2021%' or r.notes like '%Flex%2021%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,new_normal as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as newnormal_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%NEWNORMAL%' or r.notes like '%NEW%NORMAL%' or r.notes like '%NewNormal%' or r.notes like '%New%Normal%' or r.notes like '%newnormal%' or r.notes like '%new%normal%' or r.notes like '%Newnormal%' or r.notes like '%New%normal%'  or r.notes like '%NEW_NORMAL%' or r.notes like '%New_Normal%' or r.notes like '%new_normal%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,badapple as (
    select distinct 
        r.created_at, r.updated_at, r.uuid, r.notes, count(*) as badapple_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%BADAPPLE%' or r.notes like '%BAD%APPLE%' or r.notes like '%BadApple%' or r.notes like '%Bad%Apple%' or r.notes like '%badapple%' or r.notes like '%bad%apple%' or r.notes like '%Badapple%' or r.notes like '%Bad%apple%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,sixppromo as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as sixp_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%6ppromo%' or r.notes like '%6PPROMO%' or r.notes like '%6Ppromo%' or r.notes like '%6P%PROMO%' or r.notes like '%6p%Promo%' or r.notes like '%6p%promo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,startingfrom as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as startingfrom_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%STARTINGFROMPROMO%' or r.notes ilike '%STARTING%FROM%PROMO%'or r.notes like '%StartingFromPromo%' or r.notes like '%Starting%From%Promo%' or r.notes like '%startingfrompromo%' or r.notes like '%starting%from%promo%' or r.notes like '%Startingfrompromo%' or r.notes like '%Starting%from%promo%' or r.notes like '%STARTINGFROM%PROMO%' or r.notes like '%Starting%from%Promo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,startingat299 as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as starting299_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%STARTINGAT299%' or r.notes like '%StartingAt299%' or r.notes like '%startingat299%' or r.notes like '%Startingat299%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,DDinternational as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as DDinternational_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%2022DDINTERNATIONAL%' or r.notes like '%2022DDInternational%' or r.notes like '%2022ddinternational%' or r.notes like '%2022%DDINTERNATIONAL%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,anzwinterrenewal as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as anzwinter_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%ANZWINTERRENEWAL%' or r.notes like '%ANZWinterRenewal%' or r.notes like '%anzwinterrenewal%' or r.notes like '%ANZ%WINTER%RENEWAL%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,wallarkaden as (
    select distinct 
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as wallarkaden_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%OPENINGWALLARKADEN%' or r.notes like '%OpeningWallarkaden%' or r.notes like '%openingwallarkaden%' or r.notes like '%OPENING%WALLARKADEN%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,winterpromo as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as winter_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%WINTERPROMO%' or r.notes ilike '%winterpromo%' or r.notes ilike '%WinterPromo%' or r.notes ilike '%WINTER%PROMO%' or r.notes ilike '%Winter%Promo%' or r.notes ilike '%winter%promo%' or r.notes ilike '%Winter%promo%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,memberupgrade as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as member_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%MEMBERUPGRADE%' or r.notes ilike '%memberupgrade%' or r.notes ilike '%MemberUpgrade%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,expandwithus as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as expand_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%EXPANDWITHUS%' or r.notes ilike '%expandwithus%' or r.notes ilike '%ExpandWithUs%' or r.notes ilike '%EXPAND%WITH%US%' or r.notes ilike '%Expand%With%Us%' or r.notes ilike '%expand%with%us%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,koreagoodapple as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as goodapple_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%KOREAGOODAPPLE%' or r.notes ilike '%koreagoodapple%' or r.notes ilike '%KoreaGoodApple%' or r.notes ilike '%KOREA%GOOD%APPLE%' or r.notes ilike '%Korea%Good%Apple%' or r.notes ilike '%korea%good%apple%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,monthsfree as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as monthsfree_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%3FREEMONTHS%' or r.notes ilike '%3freemonths%' or r.notes ilike '%3FreeMonths%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,churchill as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as churchill_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%CHURCHILLUPGRADE%' or r.notes ilike '%ChurchillUpgrade%' or r.notes ilike '%churchillupgrade%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,threefree as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as threefree_tag
    from fivetran.spaceman_public.reservations r
    where r.notes ilike '%THREEFREE%' or r.notes ilike '%ThreeFree%' or r.notes ilike '%threefree%'
    group by r.created_at, r.updated_at,r.uuid,r.notes
)

,hundred_harris as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as hundredharris_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%100%HARRIS%' or r.notes like '%100%harris%' or r.notes like '%100%Harris%' or r.notes like '%100Harris%' or r.notes like '%100HARRIS%' or r.notes like '%100harris%'
    group by r.created_at,r.updated_at,r.uuid,r.notes
)

,hundred_harris4P as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as hundredharris4p_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%100%HARRIS%4P%' or r.notes like '%100%harris%4p%' or r.notes like '%100%Harris%4P%' or r.notes like '%100Harris4P%' or r.notes like '%100HARRIS4P%' or r.notes like '%100harris4p%'
    group by r.created_at,r.updated_at,r.uuid,r.notes
)

,skuvacancy as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as skuvacancy_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%USCSKUVACANCY%' or r.notes like '%USCSKUVacancy%' or r.notes like '%USC%SKU%VACANCY%' or r.notes like '%uscskuvacancy%' or r.notes like '%usc%sku%vacancy%' or r.notes like '%USC%SKU%Vacancy%'
    group by r.created_at,r.updated_at,r.uuid,r.notes
)

,intl_starting_from as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as intlstartingfrom_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%intl%starting%from%' or r.notes like '%INTLSTARTINGFROM%' or r.notes like '%STARTINGFROMINTL%' or r.notes like '%INTL%STARTING%FROM%' 
    group by r.created_at,r.updated_at,r.uuid,r.notes
)       
     
,m2m_upgrade as (
    select distinct
        r.created_at, r.updated_at,r.uuid, r.notes, count(*) as m2mupgrade_tag
    from  fivetran.spaceman_public.reservations r
    where r.notes ilike '%m2m%upgrade%' or r.notes like '%M2MUPGRADE%' or r.notes like '%m2mupgrade%' or r.notes like '%monthtomonth%upgrade%' 
        and r.created_at >= '2023-04-01'
    group by r.created_at,r.updated_at,r.uuid,r.notes
)

,exitrelo as (
    --Pulls the list of Deal_UUIDs associated with Relocation Deals performed between 2022-10-01 and 2022-12-01, when a number of building exits were executed and a large number of members were relocated to other buildings at very low ARPMs
    SELECT DISTINCT 
        deal_uuid 
    FROM central.transformation_office.sales_comp_deal_desks d 
    LEFT JOIN central.cdm.reservables r
        ON d.reservable_uuid = r.uuid
    INNER JOIN fivetran.google_sheets.usc_building_exits_q_422_spiff s 
        ON d.location_uuid = s.location_uuid 
        AND d.initial_sale_date >= s.member_communication_date  
        AND included_in_q_4_spiff
        AND sales_reporting_month BETWEEN '2022-10-01' AND '2022-12-01' 
        AND deal_type = 'Transfer' 
        AND CONTAINS(CASE WHEN s.floor_uuids IS NOT NULL THEN s.floor_uuids ELSE r.floor_uuid END, r.floor_uuid)
)
         
select distinct
    ROW_NUMBER() over (order by a.sales_reporting_month,a.deal_uuid,a.primary_reservation_uuid) as Row_Number
    ,a.sales_reporting_month
    ,a.initial_sale_date
    ,coalesce(a.resulting_commitment_term_start,reservation_start_date) as move_in_date
    ,a.reservation_start_date
    ,a.reservation_end_date
    ,a.resulting_commitment_term_end as term_end_date
    ,a.region
    ,a.subregion
    ,a.territory
    ,l.country
    ,a.sales_market
    ,l.market
    ,a.city
    ,a.location_name
    ,a.location_uuid
    ,concat(building_code,' - ',a.location_name) as building_code_and_name
    ,n3m_AddOccup
    ,n3m_AddCap
    ,occ_current
    ,cap_current
    ,a.deal_uuid
    ,a.deal_type
    ,sf.referral_type
    ,a.reservation_deal_activity
    ,a.salesforce_opportunity_id
    ,a.is_renewal_deal
    ,case when a.deal_uuid = deal_exclusions.deal_uuid and reason_for_exclusion = 'Sublease Deal' then 'Sublease Deal' else 'Not Sublease Deal' end as incl_excl_sublease
    ,a.sales_account_name
    ,a.billing_account_name
    ,a.reservable_name
    ,a.product_grouping
    ,a.reservable_uuid
    ,a.primary_reservation_uuid
    ,a.commitment_bucket
    ,a.sku
    ,case when a.sku = 'DD' then 'DD'
        else a.sku_group
    end as sku_group
    ,SKU_occ
    ,SKU_capacity
    ,CASE WHEN a.reservation_deal_activity NOT IN ('End PrimaryReservation','End CommitmentTerm','Cancel PrimaryReservation')
          THEN a.primary_reservation_capacity_today 
          ELSE NULL
     END as resulting_desks
    ,sum(resulting_desks) over(partition by a.sales_reporting_month, a.deal_uuid order by 1) as deal_capacity
    ,CASE WHEN deal_capacity between 1 and 9 THEN '1-9P'
          WHEN deal_capacity between 10 and 49 THEN '10-49P'
          ELSE '50P+'
     END as deal_capacity_group
    ,a.primary_reservation_capacity
    ,a.resulting_commitment_term_months
    ,a.desk_months - zeroifnull(free.disc_desk_months) as desk_months_xfm
    ,zeroifnull(free.applied_months) as applied_free_months
    ,a.desk_months
    ,br.budget_rate
    ,ba.vat_adjust_factor
    ,reservation_price_usd
    ,reservation_price_local
    ,coalesce(cbp.recommended_discount,0) as max_discount
    ,a.reservation_price_usd * (1-COALESCE(recommended_discount,0)) * resulting_commitment_term_months as floor_price_usd
    ,a.reservation_price_local * (1-COALESCE(recommended_discount,0)) * resulting_commitment_term_months as floor_price_local
    ,core_committed_value_usd
    ,core_committed_value_local
    ,a.gross_desk_sales
    ,a.new_desk_sales
    ,a.renewal_desk_sales
    , 1 - (div0(core_committed_value_usd,(reservation_price_usd*resulting_commitment_term_months))) as avg_disc
    , max_discount - avg_disc as Disc_Difference
    , case 
        when Disc_Difference < -0.002 then 'OOP'
        else 'In Policy' 
      end as OOP
    , case
        when n3m_Occupancy <.6 
            then '<60%'
        when n3m_Occupancy >=.6 and n3m_Occupancy <.7
            then '60-69%'
        when n3m_Occupancy >=.7 and n3m_Occupancy <.8
            then '70-79%'
        when n3m_Occupancy >=.8 and n3m_Occupancy <.9
            then '80-89%'
        when n3m_Occupancy >=.9
            then '90%+'
        end as occupancy_group  
    ,deals_and_emails.email_list
    ,applied_discount
    ,case 
        --if the deal_uuid is in the exitrelo CTE, then tag it with the promo code Q422_EXITRELO
        when exitrelo.deal_uuid is not NULL then 'Q422_EXITRELO'
        --if it's not in the exitrelo CTE, then if the applied_discount field from the promo_codes CTE is NULL or not LIKE the phrases identified below, then look at the reference the Promo Code-specific CTEs mentioned above
        when applied_discount is NULL 
            or (applied_discount not like '%GROWWITHUS%'
                    and applied_discount not like '%Q3SPACEPROMO%'
                    and applied_discount not like '%GROWTHCAMPUS%'
                    and applied_discount not like '%USCSMALLUPGRADES%'
                    and applied_discount not like '%AUQ4PROMO%'
                    and applied_discount not like '%Q4SPACAEPROMO%'
                    and applied_discount not like '%Q4RENEWAL%'
                    and applied_discount not like '%Q1TOURPROMO%'
                    and applied_discount not like '%SEAGROWTHCAMPUS%'
                    and applied_discount not like '%NE&CEEGROWTHCAMPUS%'
                    and applied_discount not like '%UKEMGROWTHCAMPUS%'
                    and applied_discount not like '%SEGROWTHCAMPUS%'
                    and applied_discount not like '%ANZGROWTHCAMPUS%'
                    and applied_discount not like '%YEAROFTHETIGER%'
                    and applied_discount not like '%UKRAINE50%'
                    and applied_discount not like '%UKRAINE100%'
                    and applied_discount not like '%SUMMERSPACEPROMO%'
                    and applied_discount not like '%BUNDLE2022%'
                    and applied_discount not like '%6PPROMO%'
                    and applied_discount not like '%BADAPPLE%'
                    and applied_discount not like '%FLEX2020%'
                    and applied_discount not like '%FLEX2021%'
                    and applied_discount not like '%NEW_NORMAL%'
                    and applied_discount not like '%STARTINGFROMPROMO%'
                    and applied_discount not like '%STARTINGAT299%'
                    and applied_discount not like '%2022DDINTERNATIONAL%'
                    and applied_discount not like '%ANZWINTERRENEWAL%'
                    and applied_discount not like '%OPENINGWALLARKADEN%'
                    and applied_discount not like '%WINTERPROMO%'
                    and applied_discount not like '%MEMBERUPGRADE%'
                    and applied_discount not like '%EXPANDWITHUS%'
                    and applied_discount not like '%KOREAGOODAPPLE%'
                    and applied_discount not like '%3FREEMONTHS%'
                    and applied_discount not like '%CHURCHILLUPGRADE%'
                    and applied_discount not like '%THREEFREE%'
                    and applied_discount not like '%100HARRIS%'
                    and applied_discount not like '%100HARRIS4P%'
                    and applied_discount not like '%USCSKUVACANCY%'
                    and applied_discount not like '%YEAROFTHERABBIT%'
                    and applied_discount not like '%BIFCUSPACE%'
                    and applied_discount not like '%SSQBADAPPLE%'
                    and applied_discount not like '%250OFFKR%'
                    and applied_discount not like '%350OFFKR%'
                    and applied_discount not like '%JAKARTA23%'
                    and applied_discount not like '%INTLSTARTINGFROM%'
                    and applied_discount not like '%M2MUPGRADE%'
                )            
        then
            --if the promo code-specific CTEs have found at least 1 notes field with a phrase identified, tag the reservation with the corresponding promo code
            case when growwithus_tag>=1 then 'GROWWITHUS' 
                when q3space_tag >= 1 then 'Q3SPACEPROMO'
                when growthcampus_tag >= 1  then 'GROWTHCAMPUS'
                when uscsmall_tag >= 1 then 'USCSMALLUPGRADES'
                when auq4_tag >= 1 then 'AUQ4PROMO'
                when q4space_tag >= 1 then 'Q4SPACEPROMO'
                when q4renewal_tag >= 1 then 'Q4RENEWAL'
                when q1tour_tag >= 1 then 'Q1TOURPROMO'
                when seagrowth_tag >= 1 then 'SEAGROWTHCAMPUS'
                when neceegrowth_tag >= 1 then 'NE&CEEGROWTHCAMPUS'
                when ukemgrowth_tag >= 1 then 'UKEMGROWTHCAMPUS'
                when segrowth_tag >= 1 then 'SEGROWTHCAMPUS'
                when anzgrowth_tag >= 1 then 'ANZGROWTHCAMPUS'
                when ukraine50_tag >= 1 then 'UKRAINE50'
                when ukraine100_tag >= 1 then 'UKRAINE100'
                when tiger_tag >= 1 then 'YEAROFTHETIGER'
                when summerspace_tag >= 1 then 'SUMMERSPACEPROMO'
                when bundle2022_tag >= 1 then 'BUNDLE2022'
                when flex2020_tag>=1 then 'FLEX2020' 
                when flex2021_tag>=1 then 'FLEX2021'   
                when newnormal_tag >= 1 then 'NEW_NORMAL'
                when badapple_tag >= 1  then 'BADAPPLE' 
                when sixp_tag >= 1 then '6PPROMO'
                when startingfrom_tag >= 1 then 'STARTINGFROMPROMO'
                when starting299_tag >= 1 then 'STARTINGAT299'
                when DDinternational_tag >= 1 then '2022DDINTERNATIONAL'
                when anzwinter_tag >= 1 then 'ANZWINTERRENEWAL'
                when wallarkaden_tag >= 1 then 'OPENINGWALLARKADEN'
                when winter_tag >= 1 then 'WINTERPROMO'
                when member_tag >= 1 then 'MEMBERUPGRADE'
                when expand_tag >= 1 then 'EXPANDWITHUS'
                when goodapple_tag >= 1 then 'KOREAGOODAPPLE'
                when monthsfree_tag >= 1 then '3FREEMONTHS'
                when churchill_tag >= 1 then 'CHURCHILLUPGRADE'
                when threefree_tag >= 1 then 'THREEFREE'
                when hundredharris_tag >= 1 then '100HARRIS'
                when hundredharris4p_tag >= 1 then '100HARRIS4P'
                when skuvacancy_tag >= 1 then 'USCSKUVACANCY'
                when yearoftherabbit_tag >=1 then 'YEAROFTHERABBIT'
                when bifcuspace_tag >=1 then 'BIFCUSPACE' 
                when ssqbadapple_tag >=1 then 'SSQBADAPPLE'
                when offkr250_tag >=1 then '250OFFKR'
                when offkr350_tag >=1 then '350OFFKR'
                when jakarta23_tag >=1 then 'JAKARTA23'
                when intlstartingfrom_tag >=1 then 'INTLSTARTINGFROM'
                when m2mupgrade_tag >=1 then 'M2MUPGRADE'
                else 'No Pro'
             end
        --if the applied discount field is NOT NULL and it contains one of the phrases identified below, tag the reservation with the corresponding promo code
        when applied_discount like '%GROWWITHUS%' then 'GROWWITHUS'
        when applied_discount like '%Q3SPACEPROMO%' then 'Q3SPACEPROMO'
        when applied_discount like '%GROWTHCAMPUS%' then 'GROWTHCAMPUS'
        when applied_discount like '%USCSMALLUPGRADES%' then 'USCSMALLUPGRADES'
        when applied_discount like '%AUQ4PROMO%' then 'AUQ4PROMO'
        when applied_discount like '%Q4SPACAEPROMO%' then 'Q4SPACEPROMO'
        when applied_discount like '%Q4RENEWAL%' then 'Q4RENEWAL'
        when applied_discount like '%Q1TOURPROMO%' then 'Q1TOURPROMO'
        when applied_discount like '%SEAGROWTHCAMPUS%' then 'SEAGROWTHCAMPUS'
        when applied_discount like '%NE&CEEGROWTHCAMPUS%' then 'NE&CEEGROWTHCAMPUS'
        when applied_discount like '%UKEMGROWTHCAMPUS%' then 'UKEMGROWTHCAMPUS'
        when applied_discount like '%SEGROWTHCAMPUS%' then 'SEGROWTHCAMPUS'
        when applied_discount like '%ANZGROWTHCAMPUS%' then 'ANZGROWTHCAMPUS'
        when applied_discount like '%YEAROFTHETIGER%' then 'YEAROFTHETIGER'
        when applied_discount like '%UKRAINE50%' then 'UKRAINE50'
        when applied_discount like '%UKRAINE100%' THEN 'UKRAINE100'
        when applied_discount like '%SUMMERSPACEPROMO%' then 'SUMMERSPACEPROMO'
        when applied_discount like '%BUNDLE2022%' then 'BUNDLE2022'
        when applied_discount like '%6PPROMO%' then '6PPROMO'
        when applied_discount like '%BADAPPLE%' then 'BADAPPLE'
        when applied_discount like '%FLEX2020%' then 'FLEX2020'
        when applied_discount like '%FLEX2021%' then 'FLEX2021'
        when applied_discount like '%NEW_NORMAL%' then 'NEW_NORMAL'
        when applied_discount like '%STARTINGFROMPROMO%' then 'STARTINGFROMPROMO'
        when applied_discount like '%STARTINGAT299%' then 'STARTINGAT299'
        when applied_discount like '%2022DDINTERNATIONAL%' then '2022DDINTERNATIONAL'
        when applied_discount like '%ANZWINTERRENEWAL%' then 'ANZWINTERRENEWAL'
        when applied_discount like '%OPENINGWALLARKADEN%' then 'OPENINGWALLARKADEN'
        when applied_discount like '%WINTERPROMO%' then 'WINTERPROMO'
        when applied_discount like '%MEMBERUPGRADE%' then 'MEMBERUPGRADE'
        when applied_discount like '%EXPANDWITHUS%' then 'EXPANDWITHUS'
        when applied_discount like '%KOREAGOODAPPLE%' then 'KOREAGOODAPPLE'
        when applied_discount like '%3FREEMONTHS%' then '3FREEMONTHS'
        when applied_discount like '%CHURCHILLUPGRADE%' then 'CHURCHILLUPGRADE'
        when applied_discount like '%THREEFREE%' then 'THREEFREE'
        when applied_discount like '%100HARRIS' then '100HARRIS'
        when applied_discount like '%100HARRIS4P%' then '100HARRIS4P'
        when applied_discount like '%USCSKUVACANCY%' then 'USCSKUVACANCY'
        when applied_discount like '%YEAROFTHERABBIT%' then 'YEAROFTHERABBIT'
        when applied_discount like '%BIFCUSPACE%' then 'BIFCUSPACE'
        when applied_discount like '%SSQBADAPPLE%' then 'SSQBADAPPLE'
        when applied_discount like '%250OFFKR%' then '250OFFKR'
        when applied_discount like '%350OFFKR%' then '350OFFKR'
        when applied_discount like '%JAKARTA23%' then 'JAKARTA23'
        when applied_discount like '%INTLSTARTINGFROM%' then 'INTLSTARTINGFROM'
        when applied_discount like '%M2MUPGRADE%' then 'M2MUPGRADE'
        else 'No Promo' 
      end as promo_applied
    ,discount_reason.notes
    ,case when exitrelo.deal_uuid is not NULL then a.gross_desk_sales + 1 else a.gross_desk_sales end as gross_desk_sales_mod
    ,a._run_at
from central.cdm_sales.primary_reservation_deal_summary a
left join central.cdm.locations l
    on a.location_uuid = l.uuid
left join discount_reason
    on a.primary_reservation_uuid = discount_reason.uuid
left join occupancy1 
    on a.location_uuid = occupancy1.location_uuid
left join sku_occ 
    on a.location_uuid = sku_occ.location_uuid 
    and a.sku = sku_occ.sku
left join occupancy_current 
    on a.sales_reporting_month = occupancy_current.report_month 
    and a.location_uuid = occupancy_current.location_uuid
left join growth.growthtech_dw.dim_sf_opportunity sf
    on a.salesforce_opportunity_id = sf.sf_opportunity_id
left join deals_and_emails on a.deal_uuid = deals_and_emails.deal_uuid and a.sales_reporting_month = deals_and_emails.sales_reporting_month and a.primary_reservation_uuid = deals_and_emails.primary_reservation_uuid
LEFT JOIN free_m free
    	ON free.sales_reporting_month=a.sales_reporting_month
      	AND free.deal_uuid=a.deal_uuid
      	AND free.primary_reservation_uuid=a.primary_reservation_uuid
left join brazil_adj ba
    on a.sales_reporting_month = ba.sales_reporting_month
    and a.deal_uuid = ba.deal_uuid
    and a.primary_reservation_uuid = ba.primary_reservation_uuid
left join grow_with_us on a.sales_reporting_month = date_trunc('month',grow_with_us.created_at) and a.primary_reservation_uuid = grow_with_us.uuid
left join q3_space on a.sales_reporting_month = date_trunc('month',q3_space.created_at) and a.primary_reservation_uuid = q3_space.uuid
left join growth_campus on a.sales_reporting_month = date_trunc('month',growth_campus.created_at) and a.primary_reservation_uuid = growth_campus.uuid
left join usc_small_upgrades on a.sales_reporting_month = date_trunc('month',usc_small_upgrades.created_at) and a.primary_reservation_uuid = usc_small_upgrades.uuid
left join au_q4 on a.sales_reporting_month = date_trunc('month',au_q4.created_at) and a.primary_reservation_uuid = au_q4.uuid
left join q4_space on a.sales_reporting_month = date_trunc('month',q4_space.created_at) and a.primary_reservation_uuid = q4_space.uuid
left join q4_renewal on a.sales_reporting_month = date_trunc('month',q4_renewal.created_at) and a.primary_reservation_uuid = q4_renewal.uuid
left join q1_tourpromo on a.sales_reporting_month = date_trunc('month',q1_tourpromo.created_at) and a.primary_reservation_uuid = q1_tourpromo.uuid
left join segrowthcampus on a.sales_reporting_month = date_trunc('month',segrowthcampus.created_at) and a.primary_reservation_uuid = segrowthcampus.uuid
left join neceegrowthcampus on a.sales_reporting_month = date_trunc('month',neceegrowthcampus.created_at) and a.primary_reservation_uuid = neceegrowthcampus.uuid
left join ukemgrowthcampus on a.sales_reporting_month = date_trunc('month',ukemgrowthcampus.created_at) and a.primary_reservation_uuid = ukemgrowthcampus.uuid
left join seagrowthcampus on a.sales_reporting_month = date_trunc('month',seagrowthcampus.created_at) and a.primary_reservation_uuid = seagrowthcampus.uuid
left join anzgrowthcampus on a.sales_reporting_month = date_trunc('month',anzgrowthcampus.created_at) and a.primary_reservation_uuid = anzgrowthcampus.uuid
left join yearofthetiger on a.sales_reporting_month = date_trunc('month',yearofthetiger.created_at) and a.primary_reservation_uuid = yearofthetiger.uuid
left join ukraine50 on a.sales_reporting_month = date_trunc('month',ukraine50.created_at) and a.primary_reservation_uuid = ukraine50.uuid
left join ukraine100 on a.sales_reporting_month = date_trunc('month',ukraine100.created_at) and a.primary_reservation_uuid = ukraine100.uuid
left join summerspace on a.sales_reporting_month = date_trunc('month',summerspace.created_at) and a.primary_reservation_uuid = summerspace.uuid
left join bundle2022 on a.sales_reporting_month = date_trunc('month',bundle2022.created_at) and a.primary_reservation_uuid = bundle2022.uuid
left join flex2020 on a.sales_reporting_month = date_trunc('month',flex2020.created_at) and a.primary_reservation_uuid = flex2020.uuid
left join flex2021 on a.sales_reporting_month = date_trunc('month',flex2021.created_at) and a.primary_reservation_uuid = flex2021.uuid
left join new_normal on a.sales_reporting_month = date_trunc('month',new_normal.created_at) and a.primary_reservation_uuid = new_normal.uuid
left join badapple on a.sales_reporting_month = date_trunc('month',badapple.created_at) and a.primary_reservation_uuid = badapple.uuid
left join sixppromo on a.sales_reporting_month = date_trunc('month',sixppromo.created_at) and a.primary_reservation_uuid = sixppromo.uuid
left join startingfrom on a.sales_reporting_month = date_trunc('month',startingfrom.created_at) and a.primary_reservation_uuid = startingfrom.uuid
left join startingat299 on a.sales_reporting_month = date_trunc('month',startingat299.created_at) and a.primary_reservation_uuid = startingat299.uuid
left join DDinternational on a.sales_reporting_month = date_trunc('month',DDinternational.created_at) and a.primary_reservation_uuid = DDinternational.uuid
left join anzwinterrenewal on a.sales_reporting_month = date_trunc('month',anzwinterrenewal.created_at) and a.primary_reservation_uuid = anzwinterrenewal.uuid
left join wallarkaden on a.sales_reporting_month = date_trunc('month',wallarkaden.created_at) and a.primary_reservation_uuid = wallarkaden.uuid
left join winterpromo on a.sales_reporting_month = date_trunc('month',winterpromo.created_at) and a.primary_reservation_uuid = winterpromo.uuid
left join memberupgrade on a.sales_reporting_month = date_trunc('month',memberupgrade.created_at) and a.primary_reservation_uuid = memberupgrade.uuid
left join expandwithus on a.sales_reporting_month = date_trunc('month',expandwithus.created_at) and a.primary_reservation_uuid = expandwithus.uuid
left join koreagoodapple on a.sales_reporting_month = date_trunc('month',koreagoodapple.created_at) and a.primary_reservation_uuid = koreagoodapple.uuid
left join monthsfree on a.sales_reporting_month = date_trunc('month',monthsfree.created_at) and a.primary_reservation_uuid = monthsfree.uuid
left join churchill on a.sales_reporting_month = date_trunc('month',churchill.created_at) and a.primary_reservation_uuid = churchill.uuid
left join threefree on a.sales_reporting_month = date_trunc('month',threefree.created_at) and a.primary_reservation_uuid = threefree.uuid
left join hundred_harris on a.sales_reporting_month = date_trunc('month',hundred_harris.created_at) and a.primary_reservation_uuid = hundred_harris.uuid
left join hundred_harris4p on a.sales_reporting_month = date_trunc('month',hundred_harris4p.created_at) and a.primary_reservation_uuid = hundred_harris4p.uuid
left join skuvacancy on a.sales_reporting_month = date_trunc('month',skuvacancy.created_at) and a.primary_reservation_uuid = skuvacancy.uuid
left join YEAROFTHERABBIT on a.sales_reporting_month = date_trunc('month',YEAROFTHERABBIT.created_at) and a.primary_reservation_uuid = YEAROFTHERABBIT.uuid
left join BIFCUSPACE on a.sales_reporting_month = date_trunc('month',BIFCUSPACE.created_at) and a.primary_reservation_uuid = BIFCUSPACE.uuid
left join ssq_badapple on a.sales_reporting_month = date_trunc('month',ssq_badapple.created_at) and a.primary_reservation_uuid = ssq_badapple.uuid
left join offkr_250 on a.sales_reporting_month = date_trunc('month',offkr_250.created_at) and a.primary_reservation_uuid = offkr_250.uuid
left join offkr_350 on a.sales_reporting_month = date_trunc('month',offkr_350.created_at) and a.primary_reservation_uuid = offkr_350.uuid
left join jakarta_23 on a.sales_reporting_month = date_trunc('month',jakarta_23.created_at) and a.primary_reservation_uuid = jakarta_23.uuid
left join intl_starting_from on a.sales_reporting_month = date_trunc('month',intl_starting_from.created_at) and a.primary_reservation_uuid = intl_starting_from.uuid
left join m2m_upgrade on a.sales_reporting_month = date_trunc('month',m2m_upgrade.created_at) and a.primary_reservation_uuid = m2m_upgrade.uuid
left join exitrelo on a.deal_uuid = exitrelo.deal_uuid
left join central.cdm_finance.budget_rates br
    on a.sales_reporting_month = br.report_month
        and a.currency = br.source_currency
left join promo_codes p
    ON a.primary_reservation_uuid=p.reservation_uuid 
        AND a.RESULTING_COMMITMENT_TERM_START=p.COMMITMENT_TERM_START_UTC 
        AND a.RESULTING_COMMITMENT_TERM_END=p.COMMITMENT_TERM_END_UTC 
        AND a.sales_reporting_month <= a.RESULTING_COMMITMENT_TERM_START
        and p.rank = 1 --rank = 1 to ensure we are only pulling the promo code from the most recent term of member's reservation
left join revops.revops_dw.discount_snapshot cbp
    on a.reservable_uuid = cbp.reservable_uuid
    and case when a.resulting_commitment_term_months >= 2 AND a.resulting_commitment_term_months < 7 THEN 6
                     WHEN a.resulting_commitment_term_months >= 7 AND a.resulting_commitment_term_months < 13 THEN 12
                     WHEN a.resulting_commitment_term_months >= 13 AND a.resulting_commitment_term_months < 25 THEN 24
                     WHEN a.resulting_commitment_term_months >= 25 THEN 36
                     ELSE 1
                END = cbp.term
            and(cbp.start_at <= a.initial_sale_date::datetime and (cbp.end_at > a.initial_sale_date::datetime or cbp.end_at is null))
left join deal_exclusions
    on a.deal_uuid = deal_exclusions.deal_uuid
    and a.primary_reservation_uuid = deal_exclusions.reservation_uuid
where 1=1
    and a.region not in ('China','India')
    and a.sales_reporting_month >= '2019-06-01'
    AND (a.product_grouping IN ('SharedOfficeDesk','Office','DedicatedDesk','PrivateAccessArea','SharedOfficeDesk','HeadquartersByWework','HQxWW','office') or a.product_grouping is NULL)
    and a.reservation_deal_activity NOT IN ('End PrimaryReservation','End CommitmentTerm','Cancel PrimaryReservation')
    and (a.reservable_name not in ('All Access','WeWork Anywhere Membership') or a.reservable_name is NULL)
    AND a.product != 'HD'
    and gross_desk_sales_mod > 0 --removes sales transactions that did not result in at least 1 gross desk sale
    and (NOT COALESCE(is_wework_inc_affiliates, FALSE) OR is_wework_inc_affiliates IS NULL)
    and l.territory <> 'Mid-West'
    and case when a.deal_uuid = deal_exclusions.deal_uuid and reason_for_exclusion = 'Renewal Contract Upload Error' then 'Exclude' else 'Include' end = 'Include'
order by row_number