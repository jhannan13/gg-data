select
  lp.user_key
  , lp.tournament_week_key
  , lp.kd_ratio
  , row_number() over (partition by tournament_week_key order by kd_ratio desc) as kd_rank
  , ntile(4) over (partition by tournament_week_key order by kd_ratio desc) as quartile
  , lp.kills
  , lp.deaths
  , lp.wins

from {{ ref('lifetime_gameplay') }} as lp
