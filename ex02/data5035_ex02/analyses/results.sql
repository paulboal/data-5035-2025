/*
 * Pull the results from our summary table.
 */
SELECT *
FROM {{ ref('dq_summary') }}
PIVOT (SUM(dq_result) FOR dq_check IN (ANY ORDER BY dq_check))
ORDER BY donation_id