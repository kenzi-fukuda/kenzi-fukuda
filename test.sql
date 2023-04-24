select distinct 
    *
from central.cdm.space_inventory_bom
where location_name = '12 E 49th St'
    and report_month = '2023-05-01'

;

select DISTINCT
*
FROM central.cdm.space_inventory_bom
where location_uuid = 'c3cf2fb0-cf3a-4492-a8df-7a9478df671e'
order by report_month