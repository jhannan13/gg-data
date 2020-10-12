select
  tournament_week_key
  , game_title_key
  , first_place
  , second_place
  , third_place
  , fourth_place
  , top_ten
  , top_fifteen
  , top_twenty_five
  , kill as points_per_kill
  , slaying_out as slaying_out_bonus


from {{ source('source', 'tournament_rules') }}
