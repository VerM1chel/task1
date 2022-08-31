create_database_query = "CREATE DATABASE IF NOT EXISTS task_1"

# create_students_index = """
# CREATE INDEX rooms_index
# ON students (room)
# """

# create_students_index = """
# CREATE INDEX rooms_index
# ON students (id)
# """

first_query = """
SELECT room, 
    COUNT(id)
FROM task_1.students
GROUP BY room
"""

second_query = """
SELECT room,
	AVG(datediff(UTC_DATE, str_to_date(LEFT(birthday, 10), '%Y-%m-%d'))) as age
FROM task_1.students
GROUP BY room
ORDER BY age ASC
LIMIT 5
"""

third_query = """
SELECT room,
	max(datediff(UTC_DATE, str_to_date(LEFT(birthday, 10), "%Y-%m-%d"))) - 
		min(datediff(UTC_DATE, str_to_date(LEFT(birthday, 10), "%Y-%m-%d"))) AS age_diff
FROM task_1.students
GROUP BY room
ORDER BY age_diff desc
LIMIT 5
"""

forth_query = """
SELECT room,
	COUNT(DISTINCT(sex)) AS MF
FROM task_1.students
GROUP BY room
HAVING MF = 2
ORDER BY room
"""