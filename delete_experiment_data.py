import json

DATA_FILE="/home/xty20/dagbo/data.json"

with open(DATA_FILE) as f:
    data = json.load(f)

TO_DELETE = 3

x = len(data['trials'])

for y in range(TO_DELETE):
    del data['trials'][str(x-1)]
    del data['data_by_trial'][str(x-1)]
    x -= 1

with open(DATA_FILE, "w") as f:
    json.dump(data,f)

