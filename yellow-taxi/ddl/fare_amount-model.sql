-- Prepare data
create or replace view 
   bqml.fare_amount_stream as
select
  fare_amount,
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
CREATE or REPLACE MODEL bqml.fare_amount_model
  OPTIONS(
    model_type='linear_reg', 
    input_label_cols=['fare_amount'], 
    min_rel_progress=0.005
  ) 
AS
  select 
    * EXCEPT(class)
  from 
    `bqml.fare_amount_stream` 
  where
    class = 'TRAIN'
;
 
 
-- Create the model evaluation view
create or replace view bqml.evaluation_fare as
select
  *
from
  ML.EVALUATE(MODEL mybqml.bqml.fare_amount_model,
    (
      select * EXCEPT(class)
      from `mybqml.bqml.fare_amount_stream` 
      where class = 'TEST'
    )
  )
;
 
-- Create fare prediction view
create or replace view bqml.predicted_fare as
select 
  * EXCEPT( predicted_fare_amount),
  predicted_fare_amount,
  null as predicted_tolls_amount,
  null as predicted_extra
from
  ML.PREDICT( MODEL mybqml.bqml.fare_amount_model,
    (
      select 
        *
      from 
        `mybqml.bqml.data_staging`  
      where 
        class = 'TEST'
    )
  )