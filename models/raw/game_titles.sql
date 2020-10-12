select
  game_title_key
  , game_title_name

from {{ source('source', 'game_titles') }}
