Query- select org_name,count(org_name) from        organisations
       join offers  
       on organisations.id = offers.organisation_id
       group by org_name order by count(org_name)        desc limit 1;

Result- moaning_turquoise  10915

Query Run Time- 150 ms