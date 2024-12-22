import subprocess
import csv
import pathlib

# dag_id and run_id to filter
dag_id = 'dw_sf_3'
dag_run_id = 'manual__2024-12-22T17:10:03.962484+00:00'

sql_query = f"""
SELECT 
    ti.dag_id,
    ti.task_id,
    ti.start_date,
    ti.end_date,
    COALESCE(EXTRACT(EPOCH FROM (ti.end_date - ti.start_date)), 0)::INTEGER AS duration
FROM 
    task_instance AS ti
JOIN 
    dag_run AS dr
ON 
    ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
WHERE 
    dr.run_id = '{dag_run_id}'
ORDER BY 
    ti.start_date;
"""

# Docker command
command = [
    "docker", "exec", "-i", "src-postgres-1", 
    "psql", "-U", "airflow", "-d", "airflow", 
    "-c", sql_query
]

result = subprocess.run(command, capture_output=True, text=True)

if result.returncode == 0:
    print("Query executed successfully.")
    output = result.stdout

    rows = []
    lines = output.splitlines()

    data_started = False
    for line in lines:
        if line.startswith(" dag_id "):  # Header
            data_started = True
            continue
        if data_started:
            if line.strip() == "":
                break
            rows.append(line.strip())

    # Process rows into CSV format
    cleaned_rows = []
    for row in rows:
        columns = [col.strip() for col in row.split("|")]
        if len(columns) >= 5:  # we need all of the columns
            cleaned_rows.append(columns)

    output_dir = pathlib.Path(__file__).parent.parent / "results" / "base"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f"base_run_{dag_id}.csv"
    
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        # header
        writer.writerow(["dag_id", "task_id", "start_date", "end_date", "duration"])
        # data rows
        writer.writerows(cleaned_rows)

    print(f"\nData successfully written to {output_csv}")
else:
    print(f"Error while executing query:\n{result.stderr}")
