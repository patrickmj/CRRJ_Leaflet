<?php 

require_once 'config.php';

$pgConnection = pg_connect($connectionString);

$incidentLevelQuery = "select 
  i.incident_id
  ,i.case_abstract
  ,i.date_of_incident
  ,i.date_of_incident_indeterminate
  ,i.incident_year_range_beg
  ,i.incident_year_range_end
  ,iv.victim_names
  ,i.alleged_offense
  ,ip.perpetrator_names
  ,docs.documents
  ,cities.name as city_name
  ,counties.name as county_name
  ,states.name as state_name
  ,coalesce(cities.coordinates[0], counties.coordinates[0], states.coordinates[0]) as longitude
  ,coalesce(cities.coordinates[1], counties.coordinates[1], states.coordinates[1]) as latitude
  ,case when cities.coordinates is not null then 'city'
  		when counties.coordinates is not null then 'county'
		else 'state' end as coordinate_type
from incidents as i
left join (
  select incident_id,
	string_agg(concat_ws(' ', p.title, p.given_name, p.family_name, p.suffix), ', ') as victim_names
  from incidents_people as ip
  left join roles as role
    on ip.role_id=role.role_id
left join people as p 
	on ip.person_id=p.person_id
 where role.name='victim'
 group by incident_id) as iv
 on i.incident_id = iv.incident_id
 left join (
  select incident_id, 
  string_agg(concat_ws(' ', p.title, p.given_name, p.family_name, p.suffix), ', ') as perpetrator_names
  from incidents_people as ip
  left join roles as role
    on ip.role_id=role.role_id
  left join people as p 
  on ip.person_id=p.person_id
 where role.name='alleged perpetrator'
 group by incident_id) as ip
 on i.incident_id = ip.incident_id
left join (select incident_id, 
  string_agg(concat_ws('', 
    '<a href=',
    concat_ws('', 'https://repository.library.northeastern.edu/files/', d.document_id),
    '>', d.title, '</a>'), ', ') as documents
		  from incidents_documents as inc_docs
      left join documents as d
      on inc_docs.document_id = d.document_id
      group by incident_id) as docs
		 on i.incident_id = docs.incident_id

left join states 
on i.state_id=states.state_id
left join cities on
i.city_id=cities.city_id
left join counties on
i.county_id=counties.county_id;";


$result = pg_query($pgConnection, $incidentLevelQuery );

$resultArray = pg_fetch_all($result);
print_r( json_encode($resultArray, JSON_PRETTY_PRINT));


?>






