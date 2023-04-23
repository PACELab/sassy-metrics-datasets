import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

benchmark = "manual_mc1"
perf_data = pd.read_csv("perfparsed-" + benchmark + ".csv")
io_data = pd.read_csv("ioparsed-" + benchmark + ".csv")
rapl_data = pd.read_csv("raplparsed-" + benchmark + ".csv")
pmtrace1 = pd.read_csv("tracefile-" + benchmark + ".csv")

# processing io_data
io = {}
for i, r in io_data.iterrows():
    t = r['time'] + 1
    p = r['pid']
    if t not in io:
        io[t] = {}
    if p not in io[t]:
        io[t][p] = {'pid': p, 'blocks': 0, 'bytes': 0}
    io[t][p]['blocks'] += r['block']
    io[t][p]['bytes'] += r['byte']

# processing perf_data
# perf_data
perf = {}
for i, r in perf_data.iterrows():
    t = r['time']
    p = r['pid']
    if t not in perf:
        perf[t] = {}
    if p not in perf[t]:
        perf[t][p] = {'pid': p, 'cycles': 0, 'instructions': 0, 'ref-cycles': 0, 'LLC-load-misses': 0, 'LLC-loads': 0, 'LLC-store-misses': 0, 'LLC-stores': 0, 'blocks': 0, 'bytes': 0}
    perf[t][p][r['event']] += r['count']

# res = []
l = max(list(perf.keys())[-1], list(io.keys())[-1])
res = [{'cycles': 0, 'instructions': 0, 'ref-cycles': 0, 'LLC-load-misses': 0, 'LLC-loads': 0, 'LLC-store-misses': 0, 'LLC-stores': 0, 'blocks': 0, 'bytes': 0}] * l
for i in range(l):
    if i in perf:
        for p in perf[i]:
            res[i] = {x: res[i].get(x, 0) + perf[i][p].get(x, 0) for x in set(res[i]).union(perf[i][p])}
    if i in io:
        for p in io[i]:
            res[i] = {x: res[i].get(x, 0) + io[i][p].get(x, 0) for x in set(res[i]).union(io[i][p])}
    res[i].pop('pid', None)

df = pd.DataFrame(res)
pkg = rapl_data['pkg_pwr']
ram = rapl_data['ram_pwr']
df["pkg_pwr"] = pkg
df["ram_pwr"] = ram
df1 = pd.DataFrame()
df1['power'] = pmtrace1['power']
df = df.assign(power=df1['power'])
df = df.dropna()
# sorting columns
df = df[sorted(df.columns)]
# moving rapl values and power to the last 3 columns
cols = list(df.columns.values) 
cols.pop(cols.index('power'))
cols.pop(cols.index('pkg_pwr')) 
cols.pop(cols.index('ram_pwr'))
df = df[cols+['pkg_pwr', 'ram_pwr', 'power']]
df.to_csv("final2-" + benchmark + ".csv")