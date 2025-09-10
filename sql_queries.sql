

CREATE DATABASE video_streaming;

 CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR PRIMARY KEY,
                age INT,
                country VARCHAR(50),
                subscription_type VARCHAR(20),
                registration_date DATE,
                total_watch_time_hours NUMERIC
            );


CREATE TABLE IF NOT EXISTS viewing_sessions (
                session_id VARCHAR PRIMARY KEY,
                user_id VARCHAR REFERENCES users(user_id),
                content_id VARCHAR,
                watch_date DATE,
                watch_duration_minutes INT,
                completion_percentage NUMERIC,
                device_type VARCHAR(50),
                quality_level VARCHAR(20)
            )


ORDER BY u.country, rank_in_region;

--top 5 content by country para postgresql
WITH ranked_views AS (
    SELECT 
        u.country,
        vs.content_id,
        COUNT(vs.session_id) AS total_views,
        RANK() OVER (PARTITION BY u.country ORDER BY COUNT(vs.session_id) DESC) AS rank_in_region
    FROM viewing_sessions vs
    JOIN users u 
        ON vs.user_id = u.user_id
    GROUP BY u.country, vs.content_id
)
SELECT *
FROM ranked_views
WHERE rank_in_region <= 5
ORDER BY country, rank_in_region;

--User retention analysis by subscription type

WITH first_activity AS (
    SELECT 
        u.user_id,
        u.subscription_type,
        MIN(vs.watch_date) AS first_watch_date
    FROM users u
    JOIN viewing_sessions vs 
        ON u.user_id = vs.user_id
    GROUP BY u.user_id, u.subscription_type
),
retention AS (
    SELECT 
        f.subscription_type,
        DATE_TRUNC('month', f.first_watch_date) AS cohort_month,
        DATE_TRUNC('month', vs.watch_date) AS active_month,
        COUNT(DISTINCT f.user_id) AS retained_users
    FROM first_activity f
    JOIN viewing_sessions vs 
        ON f.user_id = vs.user_id
       AND vs.watch_date >= f.first_watch_date
    GROUP BY f.subscription_type, cohort_month, active_month
)
SELECT 
    subscription_type,
    cohort_month,
    active_month,
    retained_users
FROM retention
ORDER BY subscription_type, cohort_month, active_month;

/*Revenue Analysis by Content Genre, este no funciono porque no se tiene 
una tabla con data de los contenidos, osea el JSON tiene eso pero esta en mongo*/

SELECT 
    c.genre,
    u.subscription_type,
    COUNT(DISTINCT vs.user_id) AS total_viewers,
    COUNT(vs.session_id) AS total_views,
    SUM(
        CASE 
            WHEN u.subscription_type = 'premium' THEN 15.00
            WHEN u.subscription_type = 'standard' THEN 10.00
            WHEN u.subscription_type = 'basic' THEN 5.00
            ELSE 0
        END
    ) AS estimated_revenue
FROM viewing_sessions vs
JOIN users u 
    ON vs.user_id = u.user_id
JOIN content c 
    ON vs.content_id = c.content_id
GROUP BY c.genre, u.subscription_type
ORDER BY estimated_revenue DESC;

--Seasonal Viewing Patterns

SELECT 
    DATE_TRUNC('month', vs.watch_date) AS month,
    COUNT(vs.session_id) AS total_views,
    SUM(vs.watch_duration_minutes) AS total_minutes_watched,
    AVG(vs.completion_percentage) AS avg_completion_rate
FROM viewing_sessions vs
GROUP BY DATE_TRUNC('month', vs.watch_date)
ORDER BY month;

--Device Preference Correlation with Completion Rates

SELECT 
    vs.device_type,
    COUNT(vs.session_id) AS total_views,
    AVG(vs.completion_percentage) AS avg_completion_rate,
    AVG(vs.watch_duration_minutes) AS avg_watch_duration
FROM viewing_sessions vs
GROUP BY vs.device_type
ORDER BY avg_completion_rate DESC;