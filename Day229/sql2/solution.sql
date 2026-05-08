WITH all_teams AS (

    SELECT 
        team_a AS team,
        winner
    FROM matches

    UNION ALL

    SELECT 
        team_b AS team,
        winner
    FROM matches
)

SELECT
    team,
    COUNT(*) AS matches_played,

    SUM(CASE 
            WHEN team = winner THEN 1 
            ELSE 0 
        END) AS matches_won,

    SUM(CASE 
            WHEN team <> winner THEN 1 
            ELSE 0 
        END) AS matches_lost

FROM all_teams
GROUP BY team
ORDER BY team;