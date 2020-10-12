select
  user_key
  , game_title_key
  , tournament_week_key

from {{ source('source', 'tournament_roster') }}
