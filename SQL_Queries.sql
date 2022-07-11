Question 1
Query- select count (*)  from organisations

Result- 100

Query Run Time - 37ms

*********************************
Question 2
Query- select count(*) from organisations
       join offer_requests 
       on organisations.id <>        offer_requests.organisation_id;

Result- 5940000

Query Run Time - 1.52s

******************************
Question 3
Query- select count(*) from organisations
       join offer_requests 
       on organisations.id =             offer_requests.organisation_id;

Result- 60000

Query Run Time - 62ms

******************************
Question 4
Query- select count(*) from organisations
       join offers  
        on organisations.id <>        offers.organisation_id;

Result- 25779996

Query Run Time- 2.890s

*****************************
Question 5
Query- select org_name,count(org_name) from        organisations
       join offers  
       on organisations.id = offers.organisation_id
       group by org_name order by count(org_name)        desc limit 1;

Result- moaning_turquoise  10915

Query Run Time- 150 ms

**********************************
Question 6
Query- select org_name,count(org_name) from        organisations
       join offers  
       on organisations.id = offers.organisation_id
       group by org_name order by count(org_name)        limit 1;

Result- native_lime  420

Query Run Time- 143 ms

**********************************
Question 7
Query - select org_name,count(org_name) from         organisations
        join orders 
        on organisations.id = orders.organisation_id
        group by org_name order by count(org_name)         desc limit 1;

Result- dual_sapphire 32

Query Result Time- 38ms

**********************************
Question 8
Query - select org_name,count(org_name) from                 organisations
        join orders 
        on organisations.id = orders.organisation_id
        group by org_name order by count(org_name)            limit 1;

Result- hollow_amber  1

Query Result Time- 38ms

******************************
Question 9
Query- select org_name,sum(total_amount) from        organisations
       join offers 
       on organisations.id = offers.organisation_id
       group by org_name order by count(org_name)        desc limit 1;

Result- moaning_turquoise  324947704

Query Run Time- 163ms

********************************
Question 10
Query- select org_name,sum(total_amount) from        organisations
       join orders  
       on organisations.id = orders.organisation_id
       group by org_name order by count(org_name)        desc limit 1;

Result- dual_sapphire  1123713

Query Run Time- 86ms