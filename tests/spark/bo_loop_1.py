import argparse
import json
import ray
from ray import tune
from ray.tune.search.ax import AxSearch
from ray.tune.search.hyperopt import HyperOptSearch
from ray.tune import Callback
"""
An integration test of the Spark DAG in a tuning loop
1. Register Spark model with Ax
2. Define search space and metrics
3. Create experiment controller
4. Initialise with bootstrap evaluations
5. Run the tuner
  a. Define and fit the Spark DAG model
  b. Optimise the acquisition
  c. Evaluate next configuration (call to Spark is stubbed)
  d. Save the result and repeat
"""

parser = argparse.ArgumentParser()
parser.add_argument('--use_bo', action='store_true')
args = parser.parse_args()

# Define the search space
search_space = {
    "spark.executor.cores": tune.sample_from(lambda config: 
                                tune.uniform(config['spark.task.cpus'], 8)),
    "spark.executor.memory": tune.randint(512, 7500),
    "spark.task.cpus": tune.randint(1, 8),
    "spark.memory.fraction": tune.randint(512, 7500),
    "spark.shuffle.compress": tune.choice([True, False]),
    "spark.shuffle.spill.compress": tune.choice([True, False])
}

SPARK_CONF_FILE_PATH = "/home/xty20/HiBench/conf/spark.conf"

def eval_fun(parameterization): 

    spark_response = {k:((0, float('nan'))) for k in metric_keys}

    with open(SPARK_CONF_FILE_PATH, "r") as f:
        lines = f.readlines()

    for key in parameterization.keys():
        to_check = key
        if (key == "spark.executor.cores"):
            to_check = "hibench.yarn.executor.cores"
        for i in range(len(lines)):
            if to_check in lines[i]:
                lines[i] = to_check + " " + str(parameterization[key]) + ("m" if key == "spark.executor.memory" else "") + "\n"
                break

    with open(SPARK_CONF_FILE_PATH, "w") as f:
        f.writelines(lines)
        f.flush()

    # Run a command
    result = subprocess.run(["/home/xty20/dagbo/run_spark_sql_aggregation.sh"], capture_output=True, text=True)

    # Get the exit code
    exit_code = result.returncode

    if exit_code != 0:
        raise Exception("App did not run successfully!")

    try: 
        application_response = requests.get("http://localhost:18080/api/v1/applications")
    except:
        raise Exception("Spark History Server is not running")
    
    application = application_response.json()

    app_id = None
    for app in application:
        if app["name"] == "ScalaAggregation":
            app_id = str(app["id"])
            break
    if app_id == None:
        raise Exception("Invalid App ID!")
        
    app_basic_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id).json()
    if not app_basic_info["attempts"][0]["completed"]:
        raise Exception("Spark run not completed!")
    
    executor_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id + "/executors").json()
    if len(executor_info) < 2:
        raise Exception("Invalid executors!")

    stage_0_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id + "/stages/0")
    stage_0 = stage_0_info.json()[0]

    if(stage_0["status"] != "COMPLETE"):
        import pdb
        pdb.set_trace()

    stage_1_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id + "/stages/1")
    stage_1 = stage_1_info.json()[0]

    if(stage_1["status"] != "COMPLETE"):
        import pdb
        pdb.set_trace()

    file_path = '/home/xty20/HiBench/report/hibench.report'

    # Read the file into a DataFrame
    # The regex '\s+' matches one or more whitespace characters
    df = pd.read_csv(file_path, delim_whitespace=True, header=None, engine='python')

    throughput = None

    for _, row in df.iterrows():
        if row[0] == "ScalaSparkAggregation":
            throughput = row[5]
            break

    spark_response["throughput_from_first_job"] = (throughput , float('nan'))
    
    return spark_response

class JsonLoggerCallback(Callback):
    def __init__(self, filename):
        self.filename = filename
        self.data = []

    def on_trial_complete(self, iteration, trials, trial, **info):
        trial_data = {
            "trial_id": trial.trial_id,
            "params": trial.config,
            "performance": trial.last_result
        }
        self.data.append(trial_data)

        # Save to JSON file
        with open(self.filename, 'w') as f:
            json.dump(self.data, f, indent=4)
    
metric_keys = ["throughput_from_first_job"]

# Initialize Ray
ray.init(ignore_reinit_error=True)

# Define and run the experiment
analysis = tune.run(
    eval_fun,
    config=search_space,
    num_samples=60,
    search_alg=AxSearch() if args.use_bo else HyperOptSearch(),
    callbacks=[JsonLoggerCallback("tuning_results.json")]
)

# Get the best parameters
best_parameters = analysis.best_config
print("Best parameters: ", best_parameters)

# Shut down Ray
ray.shutdown()