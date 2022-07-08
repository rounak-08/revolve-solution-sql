Query- select org_name,sum(total_amount) from        organisations
       join orders  
       on organisations.id = orders.organisation_id
       group by org_name order by count(org_name)        desc limit 1;

Result- dual_sapphire  1123713

Query Run Time- 86ms