import json

DATA_FILE="/home/xty20/dagbo/data.json"

with open(DATA_FILE) as f:
    data = json.load(f)

print(f"Number of trials: {str(len(data['trials']))}")
print(f"Latest trial: {str(data['data_by_trial'][str(25-1)])}")
