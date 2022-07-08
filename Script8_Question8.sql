Query - select org_name,count(org_name) from                 organisations
        join orders 
        on organisations.id = orders.organisation_id
        group by org_name order by count(org_name)            limit 1;

Result- hollow_amber  1

Query Result Time- 38ms