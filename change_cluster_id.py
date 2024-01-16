DATANODE_DIR = '/home/xty20/hadoop/dfs/datanode/current/VERSION'
NAMENODE_DIR = '/home/xty20/hadoop/dfs/namenode/current/VERSION'


clusterID = None
with open(NAMENODE_DIR, 'r') as f:
    lines = f.readlines()
    for line in lines:
        if 'clusterID' in line:
            clusterID = line
            break


with open(DATANODE_DIR, 'r') as f:
    lines = f.readlines()

for i in range(len(lines)):
    if 'clusterID' in lines[i]:
        lines[i] = clusterID

with open(DATANODE_DIR, 'w') as f:
    f.writelines(lines)
    f.flush()
