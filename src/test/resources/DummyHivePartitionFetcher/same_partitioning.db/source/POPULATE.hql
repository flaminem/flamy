INSERT OVERWRITE TABLE same_partitioning.source PARTITION(day, project)
SELECT
query_id,
MIN(time) as start_time,
MAX(time) as end_time,
MIN(environment) as environment,
MIN(schema_name) as schema_name,
MIN(table_name) as table_name,
MIN(command) as command,
LAST(command_status) OVER(ORDER BY time) as last_status,
MIN(day) as day,
MIN(project) as project
FROM logs.raw_logs
GROUP BY query_id
