with bike_point_status_events as (
      select cast('2022-03-20T06:00:00' as timestamp) as event_timestamp, '100' as bike_point_id, 7 as num_bikes_available union all
      select cast('2022-03-20T08:00:00' as timestamp) as event_timestamp, '100' as bike_point_id, 5 as num_bikes_available union all
      select cast('2022-03-20T10:00:00' as timestamp) as event_timestamp, '100' as bike_point_id, 1 as num_bikes_available union all
      select cast('2022-03-21T03:00:00' as timestamp) as event_timestamp, '100' as bike_point_id, 9 as num_bikes_available
),

events_with_lead AS (
  select
    lead(event_timestamp) over (partition by bike_point_id order by event_timestamp) as next_event_timestamp,
    *
  from bike_point_status_events
)
-- select * from events_with_lead;

,event_timestamp_with_gaps_filled AS (
  select bike_point_id, event_timestamp, filled_timestamp from events_with_lead as e
  left join
  UNNEST(GENERATE_TIMESTAMP_ARRAY(e.event_timestamp, TIMESTAMP_SUB(e.next_event_timestamp, INTERVAL 1 HOUR), INTERVAL 1 HOUR)) AS filled_timestamp
)
-- select * from event_timestamp_with_gaps_filled;

,event_timestamp_map AS (
  select bike_point_id, event_timestamp, IFNULL(filled_timestamp, event_timestamp) as filled_timestamp
  from event_timestamp_with_gaps_filled
)
-- select * from event_timestamp_map;

select
  eve.bike_point_id,
  eve.event_timestamp,
  map.filled_timestamp,
  num_bikes_available
from event_timestamp_map AS map
inner join bike_point_status_events eve
on (eve.bike_point_id = map.bike_point_id and map.event_timestamp = eve.event_timestamp)
order by bike_point_id,event_timestamp, filled_timestamp