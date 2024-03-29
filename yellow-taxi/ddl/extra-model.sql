select *, total/cnt/3 
from(
  select
  extra,
  --trip_hourofday ,
  count(extra) cnt,
  sum(count(extra)) OVER() total
  from
  `bqml.data_staging` 
  group by
  extra
)

, trip_hourofday 
order by
trip_hourofday 
;

create or replace view 
   bqml.extra_logst_stream as
select
 class,
 CAST(extra as STRING) as extra_str
 ,
--  trip_hourofday ,
 -- pickup_datetime 
-- ,trip_weekhour_fc 
 trip_dayofweek 
 ,geo_distance_miles 
,EXTRACT(TIME FROM pickup_datetime) AS trip_time
from
 mybqml.bqml.data_stream
 ;
 
 
CREATE OR REPLACE MODEL
  bqml.extra_logst_model
OPTIONS
  ( model_type='LOGISTIC_REG',
    auto_class_weights=TRUE,
--    CLASS_WEIGHTS = [STRUCT('0.5', 	1.0), STRUCT('0', 1.0), STRUCT('1', 1.0)],
    input_label_cols=['extra_str']
  ) 
AS
  select 
    * EXCEPT(class)
  from `bqml.extra_logst_stream` 
  where class = 'TRAIN'
;
  
select
  *
from
  ML.EVALUATE(
    MODEL bqml.extra_logst_model,
    (
      select 
        * EXCEPT(class)
      from `bqml.extra_logst_stream` 
      where class = 'TEST'
           #  limit 1000
    )
  )
;


	
0.925260730999823
0.8764757692137326
0.9023072429906541
0.8965247058521791
0.7475836276517964
0.9501716666666666

 
select 
  *
from
  ML.PREDICT( 
    MODEL bqml.extra_logst_model,
    (
      select *
      from `bqml.data_stream` 
      where class = 'TEST'
    )
 )
