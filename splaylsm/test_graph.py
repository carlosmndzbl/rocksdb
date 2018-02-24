import re
import matplotlib.pyplot as plt
from string import whitespace
import csv

experiment_filename = "test.cc"

def extract_int(match_obj):
    return int(match_obj.group(1))

def main():
    file = open(experiment_filename, mode='r')
    
    contents = file.read()
    
    def extract_int(varname):
        regexp = "{0}\s*=\s*(\d+)\s*;".format(varname)
        return int(re.search(regexp, contents).group(1))
        
    num_trials = extract_int("num_trials")
    time_interval = extract_int("time_interval")
    
    def extract_workload_params():
        regexp = "params\[\d+\]\[\d+\]\s*=\s*{([^;]*)}\s*;"
        params_str = re.search(regexp, contents).group(1)
        exclude = dict.fromkeys(map(ord, whitespace + "{}"))
        params_str = params_str.translate(exclude)
        params = list(map(lambda x: int(x), params_str.split(",")))
        return params
    
    workload_params = extract_workload_params()
    num_workloads = len(workload_params) // 3
    
    file.close()
    
    num_ops = 0
    workload_shift_markers = []
    
    for i in range(num_workloads):
        num_ops += workload_params[3 * i]
        workload_shift_markers.append(num_ops)
        
    workload_shift_markers.pop()
    
    num_datapoints = num_ops // time_interval
    
    splay_xs = []
    nosplay_xs = []
    
    splay_ys = []
    nosplay_ys = []
    
    csvfile = open('out.csv')
    reader = csv.DictReader(csvfile, fieldnames=["type", "op_num", "time"])
    
    for row in reader:
        x = int(row["op_num"])
        y = float(row["time"])
        
        if (row["type"] == "splay"):
            splay_xs.append(x)
            splay_ys.append(y)
        else:
            nosplay_xs.append(x)
            nosplay_ys.append(y)
    
    plt.plot(splay_xs, splay_ys, 'r', label="LSM+Cache")
    plt.plot(nosplay_xs, nosplay_ys, 'b', label="?Splay")
    plt.legend()
    plt.axis([0, max(splay_xs[-1], nosplay_xs[-1]), 0, plt.axis()[3]])
    
    for marker in workload_shift_markers:
        plt.axvline(x=marker, linestyle='--')

    plt.xlabel("Operation Number (in 1000s)")
    plt.ylabel("Disk I/O per Operation")
    plt.savefig("test.svg")
        
    
main()