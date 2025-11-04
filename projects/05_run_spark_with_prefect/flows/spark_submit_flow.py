from prefect import flow, task
import subprocess
from pathlib import Path
import datetime

# need to change the below value
# get spark home with powershell  $Env:SPARK_HOME
SPARK_HOME = Path(r"C:\Users\PLIU\Documents\Tool\spark\spark-3.5.2")
BASE_DIR = Path(r"C:\Users\PLIU\Documents\git\Seminar_workflow_automation\data")

@task(log_prints=True)
def run_spark(job_name: str, script_path: str, src_data_path: str, out_path:str):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = BASE_DIR / 'tmp' / job_name / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{job_name}_{timestamp}.log"

    cmd = [
        str(SPARK_HOME / "bin" / "spark-submit.cmd"),
        "--master", "local[4]",
        "--conf", "spark.driver.memory=4g",
        "--conf", f"spark.local.dir={BASE_DIR / 'tmp' / job_name / 'tmp'}",
        str(script_path),
        src_data_path,
        out_path,
    ]

    with log_file.open("w") as f:
        subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=False)

    return str(log_file)

@flow(name="spark_submit_flow")
def spark_flow(job_name: str, script_path: str, src_data_path: str, out_path:str):
    log_path = run_spark(job_name, script_path, src_data_path, out_path)
    print(f"Job {job_name} complete. Log file: {log_path}")

if __name__ == "__main__":
    spark_flow(
        job_name="spark_submit_wc",
        script_path=r"C:\Users\PLIU\Documents\git\Seminar_workflow_automation\projects\05_run_spark_with_prefect\spark_jobs\word_count.py",
        src_data_path=r"C:\Users\PLIU\Documents\git\Seminar_workflow_automation\data\source\word_raw.txt",
        out_path=r"C:\Users\PLIU\Documents\git\Seminar_workflow_automation\data\out\spark_submit_out"
    )
