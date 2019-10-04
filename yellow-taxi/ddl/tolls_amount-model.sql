create or replace view 
   bqml.tolls_amount_stream as
select
 class,
 tolls_amount,

 pickup_geo_snap_lattitude,
 pickup_geo_snap_longitude ,
 dropoff_geo_snap_lattitude ,
 dropoff_geo_snap_longitude 
 /*
 pickup_latitude,
 pickup_longitude ,
 dropoff_latitude ,
 dropoff_longitude 
 */
from
 mybqml.bqml.data_stream
 ;
 
CREATE or REPLACE MODEL 
  bqml.tolls_amount_model
OPTIONS
  (model_type='linear_reg', input_label_cols=['tolls_amount'], min_rel_progress=0.005) 
AS
  select 
    * EXCEPT(class)
  from `bqml.tolls_amount_stream` 
  where class = 'TRAIN';
  

select
  *
from
  ML.EVALUATE(
    MODEL  bqml.tolls_amount_model,
    (
      select 
        * EXCEPT(class)
      from `bqml.tolls_amount_stream` 
      where class = 'TEST'
           #  limit 1000
    )
  )
;

 
select 
  *
from
  ML.PREDICT( 
    MODEL bqml.tolls_amount_model,
    (
      select *
      from `bqml.data_stream` 
      where class = 'TEST'
    )
 )
