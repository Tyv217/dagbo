from ax.core.simple_experiment import SimpleExperiment
from ax.core.search_space import SearchSpace
from ax.core.parameter import RangeParameter, ChoiceParameter, ParameterType
from ax.core.parameter_constraint import ParameterConstraint
from ax.modelbridge.factory import Models, get_botorch
from ax.storage.runner_registry import register_runner
from ax.utils.common.constants import Keys
from ax import save, load
from ax.storage.json_store.decoder import simple_experiment_from_json, object_from_json
import argparse
import json

from dagbo.ax_utils import AxDagModelConstructor, register_runners

from spark_dag import SparkDag

import subprocess, requests, pandas as pd

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

# MODEL REGISTRY (for saving results)
register_runners()
class DelayedSparkDagInit:
    def __call__(self, train_input_names, train_target_names, train_inputs, train_targets, num_samples):
        return SparkDag(train_input_names, train_target_names, train_inputs, train_targets, num_samples)
register_runner(DelayedSparkDagInit)

# SEARCH SPACE
executor_cores = RangeParameter("spark.executor.cores", ParameterType.INT, lower=1, upper=8)
executor_memory = RangeParameter("spark.executor.memory", ParameterType.INT, lower=512, upper=7500)
task_cpus = RangeParameter("spark.task.cpus", ParameterType.INT, lower=1, upper=8)
memory_fraction = RangeParameter("spark.memory.fraction", ParameterType.FLOAT, lower=0.01, upper=0.99)
shuffle_compress = ChoiceParameter("spark.shuffle.compress", ParameterType.BOOL, values=[False, True], is_ordered=True)
shuffle_spill_compress = ChoiceParameter("spark.shuffle.spill.compress", ParameterType.BOOL, values=[False, True], is_ordered=True)
# order of parameters (configurable variables) is arbitrary be must be consistent throughout program
parameters = [executor_cores, executor_memory, task_cpus, memory_fraction, shuffle_compress, shuffle_spill_compress]

task_cpus_lt_executor_cores = ParameterConstraint(constraint_dict={"spark.task.cpus": 1., "spark.executor.cores": -1.}, bound=-0.5)

parameter_constraints = [task_cpus_lt_executor_cores]

search_space = SearchSpace(parameters, parameter_constraints)

# METRICS
# order of metric names must be in alphabetical order
metric_keys = sorted([
    "num_executors",
    "num_tasks_per_executor",
    "concurrent_tasks",
    "disk_bytes_spilled_0",
    "executor_cpu_time_0",
    "jvm_gc_time_0",
    "executor_noncpu_time_0",
    "duration_0",
    "disk_bytes_spilled_2",
    "executor_cpu_time_2",
    "jvm_gc_time_2",
    "executor_noncpu_time_2",
    "duration_2",
    "throughput_from_first_job",
])

SPARK_CONF_FILE_PATH = "/home/xty20/HiBench/conf/spark.conf"

# EVALUATION FUNCTION
def get_eval_fun():
    """
    Returns the function which evaluates Spark on a new configuration.
    This is a stub for the unit tests (which don't have Spark integration).
    """
    # from random import random
    # return lambda _: {k:(random(), float('nan')) for k in metric_keys}

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
            return spark_response

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
            return spark_response
            
        app_basic_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id).json()
        if not app_basic_info["attempts"][0]["completed"]:
            return spark_response
        
        executor_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id + "/executors").json()
        if len(executor_info) < 2:
            return spark_response

        executors = len(executor_info) - 1
        spark_response["num_executors"] = (executors, float('nan'))
        num_tasks_per_executor = int(parameterization["spark.executor.cores"] / parameterization["spark.task.cpus"])
        spark_response["num_tasks_per_executor"] = (num_tasks_per_executor, float('nan'))
        spark_response["concurrent_tasks"] = (num_tasks_per_executor * executors, float('nan'))

        stage_0_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id + "/stages/0")
        stage_0 = stage_0_info.json()[0]

        if(stage_0["status"] != "COMPLETE"):
            import pdb
            pdb.set_trace()

        spark_response["disk_bytes_spilled_0"] = (stage_0["diskBytesSpilled"] , float('nan'))
        spark_response["executor_cpu_time_0"] = (stage_0["executorCpuTime"] , float('nan'))
        spark_response["executor_noncpu_time_0"] = (stage_0["executorRunTime"] * 1000000 - stage_0["executorCpuTime"], float('nan'))
        spark_response["duration_0"] = (spark_response["executor_cpu_time_0"][0] + spark_response["executor_noncpu_time_0"][0] , float('nan'))
        jvm_gc_time_0 = 0
        for task in stage_0["tasks"].keys():
            jvm_gc_time_0 += stage_0["tasks"][task]["taskMetrics"]["jvmGcTime"]

        spark_response["jvm_gc_time_0"] = (jvm_gc_time_0 , float('nan'))

        stage_1_info = requests.get("http://localhost:18080/api/v1/applications/" + app_id + "/stages/1")
        stage_1 = stage_1_info.json()[0]

        if(stage_1["status"] != "COMPLETE"):
            import pdb
            pdb.set_trace()

        spark_response["disk_bytes_spilled_2"] = (stage_1["diskBytesSpilled"] , float('nan'))
        spark_response["executor_cpu_time_2"] = (stage_1["executorCpuTime"] , float('nan'))
        spark_response["executor_noncpu_time_2"] = (stage_1["executorRunTime"] * 1000000 - stage_1["executorCpuTime"], float('nan'))
        spark_response["duration_2"] = (spark_response["executor_cpu_time_2"][0] + spark_response["executor_noncpu_time_2"][0] , float('nan'))
        jvm_gc_time_2 = 0
        for task in stage_0["tasks"].keys():
            jvm_gc_time_2 += stage_0["tasks"][task]["taskMetrics"]["jvmGcTime"]

        spark_response["jvm_gc_time_2"] = (jvm_gc_time_2 , float('nan'))

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
    
    return eval_fun



parser = argparse.ArgumentParser()
parser.add_argument('--resume', type=int, default=0)
parser.add_argument('--experiment_load_file', type=str, default="spark_example_exp.json")
args = parser.parse_args()

num_bootstrap = 2
num_trials = 60 - num_bootstrap


if args.resume != 0:
    
    f = open(args.experiment_load_file)
    data = json.load(f)
    simple_exp=object_from_json(data)
        # Step 1: Load the existing experiment
    # experiment = load(args.experiment_load_file)

    # objective_name = experiment.optimization_config.objective.metric.name

    # for trial in experiment.trials.values():
    #     trial_params = trial.arm.parameters
    #     trial_result = trial.fetch_data().df['mean'].to_dict()
        
    #     new_trial = simple_exp.new_trial().add_arm(trial.arm)
    #     simple_exp.complete_trial(trial_index=new_trial.index, raw_data=trial_result)

    num_trials -= len(simple_exp.trials)
    import pdb
    pdb.set_trace()
else:
    # EXPERIMENT CONTROLLER
    simple_exp = SimpleExperiment(
        search_space=search_space,
        objective_name="throughput_from_first_job",
        name="spark_exp_example",
        evaluation_function=get_eval_fun()
    )
# BOOTSTRAP EVALUATIONS
sobol = Models.SOBOL(simple_exp.search_space)
simple_exp.new_batch_trial(generator_run=sobol.gen(num_bootstrap))
data=simple_exp.eval()

# TUNING EVALUATIONS
arc_samples = 4
model_constructor = AxDagModelConstructor(
    DelayedSparkDagInit(),
    list(simple_exp.search_space.parameters.keys()),
    metric_keys,
    num_samples=arc_samples
)
for _ in range(num_trials):
    # model fitting
    model = get_botorch(
        search_space=simple_exp.search_space,
        experiment=simple_exp,
        data=data,
        model_constructor=model_constructor
    )

    # reducing peak memory
    num_points = 1000
    batch_size = num_points // arc_samples
    model_gen_options = {Keys.OPTIMIZER_KWARGS: {"batch_limit": batch_size}}

    # acquisition optimisation
    simple_exp.new_trial(model.gen(1, model_gen_options=model_gen_options))

    # evaluation of next configuration
    data=simple_exp.eval()

    # saving results
    save(simple_exp, "spark_example_exp.json")
