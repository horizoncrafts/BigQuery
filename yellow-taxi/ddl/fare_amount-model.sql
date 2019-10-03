create or replace view 
   bqml.fare_amount_stream as
select
 fare_amount,
 pickup_latitude,
 pickup_longitude ,
 dropoff_latitude ,
 dropoff_longitude ,
 trip_weekhour_fc  ,
 geo_distance_miles ,
 class
from
 mybqml.bqml.data_stream
 ;


CREATE or REPLACE MODEL bqml.fare_amount_model
  OPTIONS
  (model_type='linear_reg', input_label_cols=['fare_amount'], min_rel_progress=0.005) 
AS
select * EXCEPT(class)
from `bqml.fare_amount_stream` 
where class = 'TRAIN';
 
 
select
  *
from
  ML.EVALUATE(MODEL  bqml.fare_amount_model,
    (
select * EXCEPT(class)
from `bqml.fare_amount_stream` 
where class = 'TEST'
     #  limit 1000
   )
 )
;
 
 
 select 
 *
 from
 ML.PREDICT( MODEL bqml.fare_amount_model,
  (
    select *
    from `bqml.data_stream` 
    where class = 'TEST'
   )
 )
;
    
