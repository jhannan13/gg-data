select
  tournament_week_key
  , game_title_key
  , tournament_name
  , timestamp_millis(week_start) as week_start_utc
  , timestamp_millis(week_end) as week_end_utc
  , notes

from {{ source('source', 'tournament_weeks') }}
