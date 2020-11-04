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
  , time_moving_pct
  , score
  , score_per_minute
  , placement
  , game_time_elapsed
  , unix_match_end
  , uno_key
  , coalesce(cast(crates_opened as int64), 0) as crates_opened
  , coalesce(cast(scavs_completed as int64), 0) as scavs_completed
  , coalesce(cast(bounties_completed as int64), 0) as bounties_completed
  , coalesce(cast(flags_completed as int64), 0) as flags_completed
  , coalesce(cast(supply_runs_completed as int64), 0) as supply_runs_completed
  , headshots
  , wall_bangs
  , gulag_kills
  , gulag_deaths

from {{ source('source', 'tournament_gameplay') }}

where game_mode in ('br_brquads', 'br_brhwnquad', 'br_89', 'br_25')
