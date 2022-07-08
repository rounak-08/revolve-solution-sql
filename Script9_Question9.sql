Query- select org_name,sum(total_amount) from        organisations
       join offers 
       on organisations.id = offers.organisation_id
       group by org_name order by count(org_name)        desc limit 1;

Result- moaning_turquoise  324947704

Query Run Time- 163ms