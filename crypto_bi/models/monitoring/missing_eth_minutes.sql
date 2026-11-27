with expected AS (
    SELECT DATEADD(minute, SEQ4(), date_trunc('hour', DATEADD(hour, -12, sysdate() ))) AS expected_minute
    FROM TABLE(GENERATOR(ROWCOUNT => 720))
)
SELECT e.expected_minute
FROM expected e
LEFT JOIN {{ref("stg_eth")}} b
    ON e.expected_minute = b.INTERVAL_BEGIN
where b.INTERVAL_BEGIN is NULL 
ORDER BY e.expected_minute

    