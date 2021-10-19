select id,side,full_name,team_name,assists,goals,points from
(select
        nhl_player_id as id,
        side,
        full_name,
        game_team_name as team_name,
        stats_assists as assists,
        stats_goals as goals,
        stats_assists + stats_goals as points,
        row_number() over(partition by nhl_player_id order by nhl_player_id desc) as id_rnk
from {{ ref('player_game_stats') }} --or whatever other table reference
where full_name is not null and nhl_player_id is not null
and stats_assists is not null and stats_goals is not null
)a
where a.id_rnk = 1