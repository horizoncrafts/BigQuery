-- Prepare data for tolls model
create or replace view 
   bqml.tolls_amount_stream as
select
  class,
  tolls_amount,
  --
  pickup_geo_snap_lattitude,
  pickup_geo_snap_longitude,
  dropoff_geo_snap_lattitude,
  dropoff_geo_snap_longitude 
from
  `mybqml.bqml.data_staging` 
;

-- Create and train model
CREATE or REPLACE MODEL bqml.tolls_amount_model
OPTIONS(
  model_type='linear_reg', 
  input_label_cols=['tolls_amount'], 
  min_rel_progress=0.005
) 
AS
  select 
    * EXCEPT(class)
  from 
    `bqml.tolls_amount_stream` 
  where 
    class = 'TRAIN'
;
  
-- Create the model evaluation view
create or replace view bqml.evaluation_tolls as
select
  *
from
  ML.EVALUATE(
    MODEL mybqml.bqml.tolls_amount_model,
    (
      select 
        * EXCEPT(class)
      from 
        `mybqml.bqml.tolls_amount_stream` 
      where 
        class = 'TEST'
    )
  )
;

-- Create tolls prediction view
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
      from `mybqml.bqml.data_staging`  
      where class = 'TEST'
    )
 )