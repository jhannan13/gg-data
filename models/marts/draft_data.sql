select
  lp.user_key
  , lp.tournament_week_key
  , lp.kd_ratio
  , row_number() over (partition by tournament_week_key order by kd_ratio desc) as kd_rank
  , ntile(4) over (partition by tournament_week_key order by kd_ratio asc) as quartile
  , lp.kills
  , lp.deaths
  , lp.wins
  , lp.games_played

from {{ ref('lifetime_gameplay') }} as lp

order by tournament_week_key, kd_rank
