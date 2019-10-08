-- materialize the view data
create or replace table bqml.data_staging
    as 
    select
        *
    from 
        bqml.tlc_yellow_trips_2015_fields
;

/*
create or replace view bqml.data_stream
    as
    select
        *
    from 
        mybqml.bqml.data_staging
    where
        trip_distance < 2 * geo_distance_miles
;
*/

-- prediction results
 create or replace view bqml.predicted_total as
  select 
    trip_key,
    trip_dayofweek ,
    trip_hourofday ,
    trip_weekhour_fc ,
    trip_distance ,
    fare_amount ,
    tolls_amount ,
    extra ,
    fare_amount + tolls_amount + extra trip_total_fare,
    sum( predicted_fare_amount ) predicted_fare_amount,
    sum( predicted_tolls_amount ) predicted_tolls_amount,
    sum( predicted_extra ) predicted_extra,
    sum( predicted_fare_amount ) + sum( predicted_tolls_amount ) + sum( predicted_extra ) predicted_total
  from
  (

    select * from mybqml.bqml.predicted_fare 

    UNION ALL

    select * from mybqml.bqml.predicted_tolls

    UNION ALL

    select * from mybqml.bqml.predicted_extra

  )
  group by
    trip_key,
    trip_dayofweek ,
    trip_hourofday ,
    trip_weekhour_fc ,
    trip_distance ,
    fare_amount ,
    tolls_amount ,
    extra 
  ;

