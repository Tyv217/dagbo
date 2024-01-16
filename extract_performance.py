import json
f = open('data_1.json')
data = json.load(f)
num_trials = len(data['trials'])

results = {}

for i in range(num_trials):
    trial_data = json.loads(data['data_by_trial'][str(i)]['value'][0][1]['df']['value'])
    for metric in trial_data['metric_name'].keys():
        if trial_data['metric_name'][metric] == "throughput_from_first_job":
            j = metric
            break

    performance = trial_data['mean'][j]
    results[i] = performance

print(results)