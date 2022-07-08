Query- select org_name,count(org_name) from        organisations
       join offers  
       on organisations.id = offers.organisation_id
       group by org_name order by count(org_name)        limit 1;

Result- native_lime  420

Query Run Time- 143 ms