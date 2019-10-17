-- Prepare data
create or replace view 
   bqml.extra_logst_stream 
as
select
  class,
  extra,
  --
  trip_hourofday,
  pickup_time,
  trip_weekhour_fc, 
  trip_dayofweek,
  geo_distance_miles,
  dropoff_time_est
from
  `mybqml.bqml.data_staging` 
 ;
 
-- Create and train model
CREATE OR REPLACE MODEL bqml.extra_logst_model
OPTIONS(
  model_type='LOGISTIC_REG',
  auto_class_weights=TRUE,
  input_label_cols=['extra']
) 
AS
  select 
    * EXCEPT(class)
  from 
    `bqml.extra_logst_stream` 
  where 
    class = 'TRAIN'
;

-- Create the model evaluation view
create or replace view bqml.evaluation_extra as
select
  *
from
  ML.EVALUATE(MODEL mybqml.bqml.extra_logst_model,
    (
      select 
        * EXCEPT(class)
      from 
        `mybqml.bqml.extra_logst_stream` 
      where 
        class = 'TEST'
    )
  )
;

-- Create the extra prediction view
create or replace view bqml.predicted_extra as
select 
  * EXCEPT( predicted_extra_probs, predicted_extra),
  null as predicted_fare_amount,
  null as predicted_tolls_amount,
  predicted_extra
from
  ML.PREDICT( MODEL mybqml.bqml.extra_logst_model,
    (
      select *
      from `mybqml.bqml.data_staging`  
      where class = 'TEST'
    )
 )

