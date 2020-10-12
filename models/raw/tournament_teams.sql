select
  tournament_week_key
  , game_title_key
  , team_number
  , captain_athlete_user_key
  , team_athlete_2_user_key
  , team_athlete_3_user_key
  , team_athlete_4_user_key
  , team_start_time
  , team_end_time

from {{ source('source', 'tournament_teams') }}
