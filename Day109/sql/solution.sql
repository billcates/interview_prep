with cte as(
    select 
        business_postal_code,
        lower(
        case 
        when split_part(business_address,' ',1) ~'^[0-9]+$'
        then split_part(business_address,' ',2)
        when split_part(business_address,' ',2) ~'^[0-9]+$'
        then split_part(business_address,' ',1)
        else split_part(business_address,' ',1)
        end 
        )as street
    from sf_restaurant_health_violations
    where business_postal_code is not NULL
)
select business_postal_code, count(distinct street) as n_streets
from cte
group by 1 
order by 2 desc, 1 asc;
