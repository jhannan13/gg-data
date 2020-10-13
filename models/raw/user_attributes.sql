select
  user_key
  , user_name
  , discord_name
  , platform
  , non_html_key
  , normalized_gamertag
  , email
  , python_tag

from {{ source('source', 'user_attributes') }}
