select
  tr.*
  , ua.platform
  , ua.python_tag
  , concat(ua.platform,'/gamer/', ua.user_key) as python_tag_clean


from {{ ref('tournament_roster') }} as tr
left join {{ ref('user_attributes') }} as ua
  on lower(tr.user_key) = lower(ua.user_key)

order by 1,3
