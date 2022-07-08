Query- select count(*) from organisations
       join offer_requests 
       on organisations.id <>        offer_requests.organisation_id;

Result- 5940000

Query Run Time - 1.52s