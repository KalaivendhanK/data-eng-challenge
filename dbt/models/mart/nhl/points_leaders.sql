select team_name,full_name,points_per_player as points
from
    (
    select team_name,full_name,points_per_player,
    rank() over (partition by team_name,full_name order by points_per_player desc) as top_point_player
    from
        (
        select
        team_name,
        full_name,
        sum(points) as points_per_player
        from {{ ref('nhl_players') }}  -- or other tables
        group by team_name, full_name
        )a
        where points_per_player >= 1
    )b
where b.top_point_player = 1
