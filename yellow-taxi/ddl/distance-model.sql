create or replace view 
   bqml.distance_lon_hour_stream as
select
 trip_distance,
 pickup_latitude,
 pickup_longitude ,
 dropoff_latitude ,
 dropoff_longitude ,
 geo_distance_miles ,
 class
from
 mybqml.bqml.data_stream
 ;


CREATE or REPLACE MODEL bqml.distance_lon_hour_model
  OPTIONS
  (model_type='linear_reg', input_label_cols=['trip_distance'], min_rel_progress=0.005) 
AS
select * EXCEPT(class)
from `bqml.distance_lon_hour_stream` 
where class = 'TRAIN';
 
 
select
  *
from
  ML.EVALUATE(MODEL  bqml.distance_lon_hour_model,
    (
select * EXCEPT(class)
from `bqml.distance_lon_hour_stream` 
where class = 'TEST'
       limit 1000
   )
 )
 ;