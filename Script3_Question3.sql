Query- select count(*) from organisations
       join offer_requests 
       on organisations.id =             offer_requests.organisation_id;

Result- 60000

Query Run Time - 62ms