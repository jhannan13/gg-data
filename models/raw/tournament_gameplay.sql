select
  tournament_week_key
  , game_title_key
  , team_number
  , athlete_user_key
  , athlete
  , match_id
  , game_mode
  , unix_match_start
  , damage_done
  , damage_taken
  , deaths
  , distance_traveled
  , kill_death_ratio
  , kills
  , team_moving_pct as time_moving_pct
  , score
  , score_per_minute
  , placement
  , game_time_elapsed

from {{ source('source', 'tournament_gameplay') }}

where game_mode = 'br_brquads'
