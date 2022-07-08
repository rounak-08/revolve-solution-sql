Query- select count(*) from organisations
       join offers  
        on organisations.id <>        offers.organisation_id;

Result- 25779996

Query Run Time- 2.890s