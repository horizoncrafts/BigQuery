create or replace table bqml.data_staging
    as 
    select
        *
    from 
        bqml.tlc_yellow_trips_2015_fields
;

create or replace view bqml.data_stream
    as
    select
        *
    from 
        mybqml.bqml.data_staging
    where
        trip_distance < 2 * geo_distance_miles
;

create or replace view bqml.data_stream_train
    as
    select
        *
    from 
        mybqml.bqml.data_stream
    where
        class = 'TRAIN'
;

create or replace view bqml.data_stream_test
as
    select
        *
    from 
        mybqml.bqml.data_stream
    where
        class <> 'TEST'


-- prediction results

create or replace view bqml.predicted_fare as
select 
  * EXCEPT( predicted_fare_amount),
  predicted_fare_amount,
  null as predicted_tolls_amount,
  null as predicted_extra
from
 ML.PREDICT( MODEL mybqml.bqml.fare_amount_model,
   (
    select *
    from `mybqml.bqml.data_stream` 
    where class = 'TEST'
   )
 )

;

create or replace view bqml.predicted_tolls as
select 
  * EXCEPT( predicted_tolls_amount),
  null as predicted_fare_amount,
  predicted_tolls_amount,
  null as predicted_extra
from
  ML.PREDICT( MODEL mybqml.bqml.tolls_amount_model,
    (
      select *
      from `mybqml.bqml.data_stream` 
      where class = 'TEST'
    )
 )

create or replace view bqml.predicted_extra as
select 
  * EXCEPT( predicted_extra_str_probs, predicted_extra_str),
  null as predicted_fare_amount,
  null as predicted_tolls_amount,
  predicted_extra
from
  ML.PREDICT( MODEL mybqml.bqml.extra_logst_model,
    (
      select *
      from `mybqml.bqml.data_stream` 
      where class = 'TEST'
    )
 )

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
    fare_amount + tolls_amount + extra trip_total,
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

