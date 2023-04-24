/*
CONTEXT:
 - One query for all of WeWork's inventory-related Global Pricing Data

 RESULT EXPECTATION:
 - This query returns the occupancy status and member pricing data if occupied of every reservable in every month of the reservable's existence, starting from 2020-01-01

 ASSUMPTION:
 - Each row should have a unique pairing of reservable_UUID and report_month, so we do not expect any duplicate rows
 - There are a list of reservations that have been excluded due to contract upload errors
 - Future months of data assume that if a member is currently occupying a space and has not either (1) renewed their commitment or (2) given notice they are leaving, they will continue their commitment on a month to month (M2M) basis at the list price at the time they signed their most recent commitment
  */


with deal_exclusions as (
    select * from fivetran.google_sheets.pricing_dash_exclusions
)

,sku_occupancy as ( 
    --Monthly Core: Building SKU Occupancy (SKUs are DD, 1-9P, 10-49P, 50P+)
    select distinct
        date,
        location_uuid,
        case
            when a.sku = 'DD' then 'DD'
            when a.capacity <=9 then '1-9P'
            when a.capacity <=49 and a.capacity >9 then '10-49P'
            when a.capacity >=50 then '50P+'
        end as sku_group,
        sum(a.occupancy) as sku_occupancy,
        sum(a.capacity) as sku_capacity
    from central.cdm.space_inventory_bom a 
    left join central.cdm.accounts using(account_uuid)
    where sku != 'HD'
        and date >= '2020-01-01'
        and (NOT COALESCE(is_wework_inc_affiliates, FALSE) OR is_wework_inc_affiliates IS NULL)
    group by date, location_uuid,sku_group
)

,location_occupancy as ( 
    --pulls Monthly Core: Building Occupancy
    select distinct
        date,
        location_uuid,
        sum(occupancy) as building_occupancy,
        sum(capacity) as building_capacity
    from central.cdm.space_inventory_bom a
    left join central.cdm.accounts using(account_uuid)
    where date >= '2020-01-01'
        and sku != 'HD'
        and (NOT COALESCE(is_wework_inc_affiliates, FALSE) OR is_wework_inc_affiliates IS NULL)
    group by 1,2
)

,renewal_flag as (
    --pulls from sales transaction data whether a deal was new business or a renewal for each term in a member's reservation
    --the transfer upgrade tag flads deals where a member transferred from one office to another and increased the number of desks they occupy in their next term
    select distinct 
        primary_reservation_uuid
        ,coalesce(cast(term_id as varchar(20)), cast(concat('M2M',sales_reporting_month) as varchar(20))) as term_id
        ,term_start_date
        ,term_end_date
        ,case when is_renewal_deal = 'TRUE' then 1 else 0 end as renewal_flag
        ,case when sales_record_type = 'Transfer In' and desk_account_impact in ('Account Expansion','Account Acquisition','Account Upgrade') then 1 else 0 end as transfer_upgrade
    from central.cdm_sales.sales_records sr
)

,renewal_flag1 as (
    --consolidates the renewal flag and the transfer upgrade flag to classify members who renewed but increased number of desks occupied as New Sale and members who renew in the same office or transfer to an office of the same or smaller size as renewals
    select distinct
        primary_reservation_uuid
        ,term_id
        ,term_start_date
        ,term_end_date
        ,case 
            when sum(renewal_flag)>=1 and sum(transfer_upgrade) >=1 then 'New Sale' 
            when sum(renewal_flag)>=1 and sum(transfer_upgrade) = 0 then 'Renewal' 
            else 'New Sale' 
        end as renewal_flag
    from renewal_flag
    group by 1,2,3,4
)

,discounts as (
    --pulls the max discount for each reservable in each month, used to calculate the max floor price for each reservable
    select distinct
        a.report_month
        ,a.reservable_uuid
        ,max(coalesce(reservable_discounts.recommended_discount,0)) as reservable_discount
    from central.cdm.space_inventory_bom a
    left join central.cdm.locations b on a.location_uuid = b.uuid
    left join revops.revops_dw.discount_snapshot reservable_discounts
        on reservable_discounts.reservable_uuid = a.reservable_uuid
        and (reservable_discounts.start_at <= a.date and (reservable_discounts.end_at >= a.date or reservable_discounts.end_at is null))
    where a.sku <> 'HD'
        and b.region not in ('China','India')
    group by 1,2
)

,q4_exits_deals as (
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
        AND CONTAINS(CASE WHEN s.floor_uuids IS NOT NULL THEN s.floor_uuids ELSE r.floor_uuid END, r.floor_uuid)
    WHERE deal_type = 'Transfer' OR (deal_type = 'Move Out Notice' AND net_commitment_term_months_changed < 0)
)

,temp as (
 --combines the deal_uuids with reservation information to easily pull into the main table
    select
        case when e.deal_uuid is not null then true else false end as is_exit_deal
        , *
    from central.transformation_office.sales_comp_deal_desks d
    left join q4_exits_deals e
        on d.deal_uuid = e.deal_uuid
    where 1=1
        and sales_reporting_month >= '2021-01-01'
        and region = 'US & Canada'
)

select distinct
    a.date,
    a.report_month,
    b.region,
    b.territory,
    b.country,
    b.market,
    a.location_name,
    a.location_uuid,
    b.ww_code,
    building_occupancy,
    a.sku,
    sku_occupancy,
    a.reservable_name,
    a.reservable_type,
    a.reservable_uuid,
    a.reservation_uuid,
    coalesce(cast(a.term_id as varchar(20)), cast(concat('M2M',a.report_month) as varchar(20))) as term_id,
    is_month_to_month, --might not be necessary with new arpm definitions
    a.account_name,
    a.account_uuid,
    a.commitment_full_duration,
    a.commitment_remaining_duration,
    date_trunc('month',A.commitment_term_created_utc::date) as sale_month, --should this be local or utc? personally a fan of utc
    date_trunc('month',A.commitment_term_start_utc::date) as start_month,
    date_trunc('month',A.commitment_term_end_utc::date) as end_month,
    case when commitment_full_duration = commitment_remaining_duration then a.capacity else 0 END as memberships,
    a.occupancy,
    a.capacity,
    a.local_currency,
    br.budget_rate,
    Renewal_Flag,
    a.usd_price as list_price_usd,
    a.local_currency_price as list_price_lcl,
    a.market_price_local,
    a.market_price_usd,
    reservable_discount,
    a.net_paid_price_usd,
    a.net_paid_price_local,
    a.market_price_usd * (1-coalesce(reservable_discount,0)) as floor_price_usd,
    a.market_price_local * (1-coalesce(reservable_discount,0)) as floor_price_lcl,
    case
        when a.sku = 'DD' then 'DD'
        when a.capacity <=9 then '1-9P'
        when a.capacity <=49 and a.capacity >9 then '10-49P'
        when a.capacity >=50 then '50P+'
    end as sku_group,
    case when is_exit_deal = TRUE then 'Relocation Deal' else NULL end as is_relocation_deal,
    case when a.reservation_uuid = deal_exclusions.reservation_uuid and reason_for_exclusion = 'Sublease Deal' then 'Sublease Deal' else 'Not Sublease Deal' end as incl_excl_sublease
from central.cdm.space_inventory_bom a
left join central.cdm.locations b 
    on a.location_uuid = b.uuid
left join central.cdm.accounts c using(account_uuid)
left join discounts
    on a.report_month = discounts.report_month
    and a.reservable_uuid = discounts.reservable_uuid  
left join sku_occupancy
    on a.date = sku_occupancy.date
    and a.location_uuid = sku_occupancy.location_uuid
    and case
        when a.sku = 'DD' then 'DD'
        when a.capacity <=9 then '1-9P'
        when a.capacity <=49 and a.capacity >9 then '10-49P'
        when a.capacity >=50 then '50P+'
    end = sku_occupancy.sku_group
left join location_occupancy 
    on a.date = location_occupancy.date 
    and a.location_uuid = location_occupancy.location_uuid
left join (
    select distinct primary_reservation_uuid, is_exit_deal from temp where is_exit_deal = TRUE
) t
    on a.reservation_uuid = t.primary_reservation_uuid
left join deal_exclusions on a.reservation_uuid = deal_exclusions.reservation_uuid
left join central.cdm_finance.budget_rates br
    on a.local_currency = br.source_currency
    and (case when year(a.report_month) <= 2022 then a.report_month = br.report_month
            when year(a.report_month) > 2022 then br.report_month = date('2022-12-01')
        end)
left join renewal_flag1
    on a.reservation_uuid = renewal_flag1.primary_reservation_uuid
        and coalesce(cast(a.term_id as varchar(20)), cast(concat('M2M',a.report_month) as varchar(20))) = renewal_flag1.term_id
        and (date_trunc('month',a.commitment_term_start_local) = date_trunc('month',renewal_flag1.term_start_date) 
         or date_trunc('month',a.commitment_term_start_utc) = date_trunc('month',renewal_flag1.term_start_date))
        and (date_trunc('month',a.commitment_term_end_local) = date_trunc('month',renewal_flag1.term_end_date) 
         or date_trunc('month',a.commitment_term_end_utc) = date_trunc('month',renewal_flag1.term_end_date))
where a.date >= '2020-01-01'
    and b.region not in ('China', 'India')
    and a.sku != 'HD'
    and (NOT COALESCE(is_wework_inc_affiliates, FALSE) OR is_wework_inc_affiliates IS NULL)
    and case when a.reservation_uuid = deal_exclusions.reservation_uuid and reason_for_exclusion = 'Renewal Contract Upload Error' then 'Exclude' else 'Include' end = 'Include'
    and b.territory <> 'Mid-West'
order by 1,2,3,4
;