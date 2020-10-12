select
  user_key
  , tournament_week_key
  , games_played
  , platform
  , contracts
  , kills
  , deaths
  , kd_ratio
  , downs
  , revives
  , score
  , time_played
  , score_per_minute
  , top_five
  , top_ten
  , top_twenty_five
  , wins
  , title
  , username
  , type
  , level

from {{ source('source', 'lifetime_gameplay') }}
