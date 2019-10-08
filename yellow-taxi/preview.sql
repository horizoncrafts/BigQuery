select
  count(*) cnt,
  sum( IF(class='TRAIN', 1, 0) ) train,
  sum( IF(class='TEST', 1, 0) ) test,
  count( DISTINCT ST_ASTEXT(pickup_geo_snap) ) pickup_snap,
  count( DISTINCT ST_ASTEXT(dropoff_geo_snap) ) dropoff_snap,
  count( DISTINCT geo_distance_miles ) distance_miles,
--  count( DISTINCT geo_distance_snap_miles ) distance_snap_miles,
  count( DISTINCT trip_distance ) trip_distance
from
bqml.data_stream
;


  select 
  extra, 
  mta_tax,
  imp_surcharge, count( 1 ) cnt
  from
   mybqml.bqml.data_stream
   group by extra, mta_tax, imp_surcharge
  LIMIT 1000
   ;


-- key
select count(DISTINCT CONCAT( CAST(pickup_datetime AS STRING), CAST(pickup_longitude  AS STRING) ) ), count(*)
from `bqml.data_stream` 
where class = 'TEST'
;