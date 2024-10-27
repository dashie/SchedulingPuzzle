import sys, copy
import os
from optparse import OptionParser
import collections


def read_and_check_one_elem_line(f, expected_element_type, error_message):
    try:
        res = expected_element_type(f.readline())
    except ValueError:
        print(error_message, file=sys.stderr)
        exit(1)
    if res <= 0:
        print(error_message, file=sys.stderr)
        exit(1)
    return res


def read_and_check_list(f, expected_length, expected_element_type, error_message):
    s = f.readline()
    res_list = s.split(" ")
    try:
        res_list = list(map(expected_element_type, res_list))
    except ValueError:
        print(error_message, file=sys.stderr)
        exit(1)
    if len(res_list) != expected_length:
        print(error_message, file=sys.stderr)
        exit(1)
    return res_list


# #####################
# num_instances
#
# for each instance (newline before each instance)
#
# N (number of tasks, int)
# N times Task Size (p_j) in scientific notation
# M (number of machines, int)
# S (number of states, int)
# S times Freq in HZ in scientific notation
# S times Pow in Watt as floats
#
#   for each task i in (1:N)
#   print predecessors in one line (as ints) or "0" if task i has no predecessor
# D (deadline , float)
# #####################
def read_instance_list(inputfile):
    data = []

    if not os.path.isfile(inputfile):
        print("ERROR: Cannot open file %s" % inputfile, file=sys.stderr)
        exit(1)

    with open(inputfile, 'r') as f:
        num_instances = read_and_check_one_elem_line(f, int,
                                                     "ERROR: Number of instances not correctly specified on the first line")

        for i in range(1, num_instances + 1):
            # skip the empty line before each instance
            f.readline()

            instance = {}

            # number of tasks
            instance["N"] = read_and_check_one_elem_line(f, int,
                                                         "ERROR: Number of tasks not correctly specified for instance %d" % i)

            # work in operations (e.g., FLOPS)
            instance["work"] = read_and_check_list(f, instance["N"], float,
                                                   "ERROR: work list not correctly specified for instance %d" % i)

            # number of machines
            instance["M"] = read_and_check_one_elem_line(f, int,
                                                         "ERROR: Number of machines not correctly specified for instance %d" % i)

            # number of states
            instance["S"] = read_and_check_one_elem_line(f, int,
                                                         "ERROR: Number of states not correctly specified for instance %d" % i)

            # list of states (frequencies)
            instance["freq_list"] = read_and_check_list(f, instance["S"], float,
                                                        "ERROR: States list not correctly specified for instance %d" % i)

            # list of power values for each state
            instance["power_list"] = read_and_check_list(f, instance["S"], float,
                                                         "ERROR: Power values list not correctly specified for instance %d" % i)

            instance["predecessors"] = {}
            for j in range(1, instance["N"] + 1):
                pred_list = f.readline().split(" ")
                try:
                    pred_list = list(map(int, pred_list))
                except ValueError:
                    print("ERROR: Predecessor list not correctly specified for task %d of instance %d" % (j, i),
                          file=sys.stderr)
                    exit(1)
                if len(pred_list) == 0:
                    print("ERROR: Predecessor list not correctly specified for task %d of instance %d" % (j, i),
                          file=sys.stderr)
                    exit(1)
                if len(pred_list) == 1 and pred_list[0] == 0:
                    # no predecessors for current task
                    continue
                instance["predecessors"][j] = pred_list

            instance["deadline"] = read_and_check_one_elem_line(f, float,
                                                                "ERROR: Deadline not correctly specified for instance %d" % i)

            data.append(instance)
    return data


def remove_empty_keys(d):
    for k in d.copy():
        if not d[k]:
            del d[k]


def diff(first, second):
    second = set(second)
    return [item for item in first if item not in second]


import string

digs = string.digits + string.ascii_letters


def int2base(x, base):
    if x < 0:
        sign = -1
    elif x == 0:
        return digs[0]
    else:
        sign = 1

    x *= sign
    digits = []

    while x:
        digits.append(digs[int(x % base)])
        x = int(x / base)

    if sign < 0:
        digits.append('-')

    digits.reverse()

    return ''.join(digits)


'''
Instance: scheduling input instance
state: state to use for scheduling all jobs
compute_energy: boolean compute energy 
'''


def schedule(instance, state: int, compute_energy: bool):
    sys.stdout = open(os.devnull, 'w')

    # processing time for each job e.g. number of operations
    p = []
    # machine load for each i-machine
    machine = {}
    # jobs not done 0
    todo = []
    # ready jobs
    ready = []
    # done jobs 0-indexed
    done = {}
    # precedence dict
    prec = {}

    for i in instance["predecessors"]:
        prec[i] = instance["predecessors"][i]

    for i in range(0, instance["M"]):
        machine[i] = 0

    for index, i in enumerate(instance["work"]):
        p.append(i / instance["freq_list"][state])
        todo.append(index + 1)

    # start scheduling
    while todo:
        # if we're at the first iteration, let's process all job without precedence constraints
        if len(todo) == instance["N"]:
            # get all jobs with precedence
            p_keys = [i for i in prec.keys()]
            # add jobs without precedence to ready list
            ready.extend(diff(todo, p_keys))

            for job in ready:
                # add to machine with smallest makespan
                m_key = min(machine, key=lambda k: machine[k])
                # increase selected machine makespan
                machine[m_key] += p[job - 1];
                done[job - 1] = [m_key, machine[m_key]];
                # remove job from list
                todo.remove(job)

        ready[:] = []  # empty ready

        # get done jobs keys
        d_keys = [i + 1 for i in done.keys()]

        # for each job in precedence dict
        for i in prec:
            diff_list = diff(prec[i], d_keys)
            # if the precedence is empty append the process to ready list since i-job is ready
            if not diff_list:
                ready.append(i)

        # now I have ready process, but we have to check times (?)
        for job in ready:
            max_c = 0;
            for i in prec[job]:
                max_c = max(max_c, done[i - 1][1])

            # FIXME tbh I don't understand why I did this
            # Abbiamo trovato il minimo makespan a cui possiamo infilare il nostro coso 
            # Ora dobbiamo trovare la macchina più conveniente in cui infilarlo

            # Find machine id with minimum makespan
            m_key = min(machine, key=lambda k: machine[k])
            if machine[m_key] >= max_c:
                # we can attach the job sequentially without idle time
                machine[m_key] += p[job - 1];
                done[job - 1] = [m_key, machine[m_key]];
            else:
                # fill machine withwith idle time
                machine[m_key] = max_c
                machine[m_key] += p[job - 1];
                done[job - 1] = [m_key, machine[m_key]];

            todo.remove(job)
            # FIXME why we pop from prec?
            prec.pop(job)

    sys.stdout = sys.__stdout__

    # PRINT SCHEDULING RESULT
    m_keys = max(machine, key=lambda k: machine[k])
    res = collections.OrderedDict(sorted(done.items()))

    if compute_energy == True:
        # compute energy as:
        # sum (energy of all the task when scheduled at state + idle times
        energy = (sum(p) * instance["power_list"][state] +
                  (instance["M"] * machine[m_keys] - sum(p)) * instance["power_list"][0])

        print(machine[m_keys])
        print(energy)
        # value: (m, makespan) -> for each job, machine where the job has been scheduled, ending makespan
        # note that the same state is used for all the jobs
        for i, v in res.items():
            print("%d %d %d %f %f" % (i + 1, v[0] + 1, state + 1, v[1] - p[i], v[1]))


if __name__ == "__main__":

    parser = OptionParser(usage="usage: %prog [options]")

    parser.add_option("-i", "--inputfile",
                      action="store",
                      dest="inputfile",
                      type="string",
                      help="path to input file")

    (options, args) = parser.parse_args()

    if options.inputfile == None or not os.path.exists(options.inputfile):
        print("Input file invalid", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    data = read_instance_list(options.inputfile)

    for instance in data:
        jobs = instance["N"]
        # schedule with maximum frequency for a feasible solution
        jobs_fixed_state = 5
        while (instance["deadline"]):
            ins = copy.deepcopy(instance)
            schedule(ins, jobs_fixed_state, True)
            break;
        print(" ")
