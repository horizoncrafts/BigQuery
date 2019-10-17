--- preview source data
select
  *
from
  `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015`
limit 100
;

select
  count(1)
from
  `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015`
;

