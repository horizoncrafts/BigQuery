create or replace table bqml_static.total_predicted 
as
select
current_timestamp() as timestamp,
'COMBINED' as label, 
*
from `mybqml.bqml.total_predicted`
;


-- Prepare data
create or replace view 
   bqml_static.total_fare_stream as
select
  trip_total_fare,
  --
  pickup_latitude,
  pickup_longitude ,
  dropoff_latitude ,
  dropoff_longitude ,
  trip_weekhour_fc  ,
  geo_distance_miles ,
  --
  class
from
  `mybqml.bqml.data_staging` 
;


-- Create and train model
CREATE or REPLACE MODEL bqml_static.total_fare_model
  OPTIONS(
    model_type='linear_reg', 
    input_label_cols=['trip_total_fare'], 
    min_rel_progress=0.005
  ) 
AS
  select 
    * EXCEPT(class)
  from 
    `bqml_static.total_fare_stream` 
  where
    class = 'TRAIN'
;
 
 
-- Create the model evaluation view
create or replace view bqml_static.evaluation_total_fare as
select
  *
from
  ML.EVALUATE(MODEL mybqml.bqml_static.total_fare_model,
    (
      select * EXCEPT(class)
      from `mybqml.bqml_static.total_fare_stream` 
      where class = 'TEST'
    )
  )
;
 
-- Create fare prediction view
create or replace view bqml_static.predicted_total_fare as
select 
  * EXCEPT( predicted_trip_total_fare),
  predicted_trip_total_fare,
  null as predicted_tolls_amount,
  null as predicted_extra
from
  ML.PREDICT( MODEL mybqml.bqml_static.total_fare_model,
    (
      select 
        *
      from 
        `mybqml.bqml.data_staging`  
      where 
        class = 'TEST'
    )
  )
;

insert into bqml_static.total_predicted 
select
  current_timestamp() as timestamp,
  'TOTAL_FARE_2' as label, 
  trip_key,
  trip_dayofweek ,
  trip_hourofday ,
  trip_weekhour_fc ,
  trip_distance ,
  fare_amount ,
  tolls_amount ,
  extra ,
  fare_amount + tolls_amount + extra trip_total_fare,
  sum( predicted_trip_total_fare ) predicted_fare_amount,
  sum( predicted_tolls_amount ) predicted_tolls_amount,
  sum( predicted_extra ) predicted_extra,
  sum( predicted_trip_total_fare ) predicted_total_fare
from `mybqml.bqml_static.predicted_total_fare` 
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


-- GEO only
-- Prepare data
create or replace view 
   bqml_static.total_fare_geo_stream as
select
  trip_total_fare,
  --
  geo_distance_miles ,
  --
  class
from
  `mybqml.bqml.data_staging` 
;


-- Create and train model
CREATE or REPLACE MODEL bqml_static.total_fare_geo_model
  OPTIONS(
    model_type='linear_reg', 
    input_label_cols=['trip_total_fare'], 
    min_rel_progress=0.005
  ) 
AS
  select 
    * EXCEPT(class)
  from 
    `bqml_static.total_fare_geo_stream` 
  where
    class = 'TRAIN'
;

create or replace view bqml_static.predicted_total_fare_geo as
select 
  * EXCEPT( predicted_trip_total_fare),
  predicted_trip_total_fare,
  null as predicted_tolls_amount,
  null as predicted_extra
from
  ML.PREDICT( MODEL mybqml.bqml_static.total_fare_geo_model,
    (
      select 
        *
      from 
        `mybqml.bqml.data_staging`  
      where 
        class = 'TEST'
    )
  )
;


insert into bqml_static.total_predicted 
select
  current_timestamp() as timestamp,
  'TOTAL_FARE_GEO' as label, 
  trip_key,
  trip_dayofweek ,
  trip_hourofday ,
  trip_weekhour_fc ,
  trip_distance ,
  fare_amount ,
  tolls_amount ,
  extra ,
  fare_amount + tolls_amount + extra trip_total_fare,
  sum( predicted_trip_total_fare ) predicted_fare_amount,
  sum( predicted_tolls_amount ) predicted_tolls_amount,
  sum( predicted_extra ) predicted_extra,
  sum( predicted_trip_total_fare ) predicted_total_fare
from `mybqml.bqml_static.predicted_total_fare_geo` 
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
