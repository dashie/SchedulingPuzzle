package scheduling.puzzle;

import java.io.PrintStream;
import java.util.*;

/**
 *
 */
public class Scheduler {

    private final int numberOfJobs;
    private final int machines;
    private final int pStates;
    private final double[] frequencies;
    private final double[] powers;
    private final Job[] jobs;
    private final double deadline;

    private double[][] jobTimePerPState;
    private double[][] jobJoulePerPState;
    private List<List<Integer>> priorityMatrix;

    /**
     *
     */
    public Scheduler(int numberOfJobs, int machines, int pStates, double[] frequencies, double[] powers, Job[] jobs, double deadline) {

        this.numberOfJobs = numberOfJobs;
        this.machines = machines;
        this.pStates = pStates;
        this.frequencies = frequencies;
        this.powers = powers;
        this.jobs = jobs;
        this.deadline = deadline;
    }

    /**
     *
     */
    public void prepare() {
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("-------");
        System.out.println();

        // prepare weights tables
        jobTimePerPState = new double[numberOfJobs][pStates];
        jobJoulePerPState = new double[numberOfJobs][pStates];
        for (int j = 0; j < numberOfJobs; ++j) {
            double ops = jobs[j].ops;
            for (int p = 0; p < frequencies.length; ++p) {
                double time = ops / frequencies[p]; // in secondds
                double joules = powers[p] * time;
                jobTimePerPState[j][p] = time;
                jobJoulePerPState[j][p] = joules;
            }
        }

        // sort dependencies by number of sub-dependencies
        for (Job job : jobs) {
            Arrays.sort(job.dependencies, (a, b) -> Integer.compare(jobs[b].dependencies.length, jobs[a].dependencies.length));
        }

        // priority matrix
        Job[] jd = Arrays.copyOf(jobs, jobs.length); // create a copy before to sort it
        priorityMatrix = evalPriorityMatrix(jd);
        dumpPriorityMatrix(priorityMatrix);
        System.out.println();
    }

    /**
     *
     */
    public void solve() {

        double maxOpsCount = estimateMaxOpsCount(0, priorityMatrix.size() - 1);
        System.out.printf("max ops count:  %f ops%n", maxOpsCount);
        System.out.printf("deadline:       %f s%n", deadline);

        int defaultPState = evalPState(0, maxOpsCount);
        if (defaultPState == 0) throw new IllegalStateException();
        System.out.printf("default pstate: %d%n", defaultPState);
        System.out.printf("ETA:            %f (%f)%n", maxOpsCount / frequencies[defaultPState], deadline);

        //
        SchedulePlan schedulePlan = scheduleJobs(defaultPState);
        System.out.println();

        //
        double totalPowerUsage = Arrays
            .stream(schedulePlan.schedules)
            .flatMap(m -> m.schedules.stream())
            .map(s -> s.powerUsage)
            .reduce(0.0, (a, b) -> a + b);

        //
        for (Machine s : schedulePlan.schedules) {
            s.dump();
        }
        System.out.println();
        System.out.printf("end time:    %15f (%f)%n", schedulePlan.endTime, deadline);
        System.out.printf("power usage: %15f%n", totalPowerUsage);
    }

    /**
     *
     */
    private int evalPState(double timeOffset, double opsCount) {
        int defaultPState = 0;
        for (int i = 1; i < frequencies.length; ++i) {
            if (timeOffset + opsCount / frequencies[i] < deadline) {
                defaultPState = i;
                break;
            }
        }
        return defaultPState;
    }

    /**
     *
     */
    private SchedulePlan scheduleJobs(int defaultPState) {
        // init Machines
        Machine[] machines = new Machine[this.machines];
        for (int i = 0; i < machines.length; ++i) {
            machines[i] = new Machine(i);
        }

        //
        int machinePtr = 0;
        for (int p = priorityMatrix.size() - 1; p >= 0; --p) {

            List<Integer> list = priorityMatrix.get(p); // jobs are sorted by sizes
            if (list.size() == 0) continue;

            // organize all level jobs per machine assigning the new one to the most free machine
            int mi = 0;
            for (int jobId : list) {
                while (machines[mi].endTime > machines[nextMachine(mi, 0)].endTime) mi = nextMachine(mi, 0);
                machines[mi].scheduleJob(jobs[jobId], p, defaultPState);
                if (machines[mi].endTime > machines[machinePtr].endTime) machinePtr = mi;
                mi = nextMachine(mi, 0); // rotate buckets to fill all uniformly
            }

            //
            double currentEndTime = machines[machinePtr].endTime;

            // fill the gaps with items from 0 priority (starting from the end of the queue)
            for (Machine machine : machines) {
                if (machine != machines[machinePtr]) {
                    double dt = currentEndTime - machine.endTime;
                    while (fillMachinePriorityGap(machine, dt, defaultPState)) { // fill the machine
                        dt = currentEndTime - machine.endTime;
                    }
                }
            }

            // TODO try to stretch machines less loaded to fill the gaps

            //

            // align end time on all machine
            for (Machine s : machines) {
                s.sync(p, currentEndTime);
            }

            // re-estimate pstate
            if (p > 0) {
                double maxOpsCount = estimateMaxOpsCount(currentEndTime, p - 1);
                if (maxOpsCount > 0) {
                    int newPState = evalPState(currentEndTime, maxOpsCount);
                    if (newPState == 0) throw new IllegalStateException();
                    if (newPState != defaultPState) {
                        System.out.printf("adjust pstate:  %d from priority job list list %d%n", newPState, p - 1);
                        defaultPState = newPState;
                    }
                }
            }
        }

        //
        return new SchedulePlan(machines, machines[machinePtr].endTime);
    }

    /**
     *
     */
    private boolean fillMachinePriorityGap(Machine machine, double dt, int pstate) {

        if (priorityMatrix.get(0).size() > 0) {
            Integer jobId = priorityMatrix.get(0).getLast();
            if (jobTimePerPState[jobId][pstate] <= dt) {
                Job job = jobs[jobId];
                // try to reduce pstate
                while (pstate > 1 && jobTimePerPState[jobId][pstate - 1] <= dt) pstate--;
                machine.scheduleJob(job, 0, pstate);
                priorityMatrix.get(0).removeLast();
                return true;
            }
        }
        return false;
    }

    /**
     *
     */
    private int nextMachine(int i, int sign) {
        // TODO Oscillate from left to right and then from right to left in the future
        return (i + 1) % this.machines;
    }

    /**
     *
     */
    private double estimateMaxOpsCount(double timeOffset, int priority) {

        double worstSize = timeOffset;
        for (int p = priority; p >= 0; --p) {
            List<Integer> list = priorityMatrix.get(p); // jobs are sorted by sizes
            // organize all level jobs per machine assigning the new one to the most free machine
            int worstBucket = 0;
            double[] cpuBucket = new double[machines];
            int bi = 0;
            for (int jobId : list) {
                while (cpuBucket[bi] > cpuBucket[(bi + 1) % machines]) bi = (bi + 1) % machines;
                cpuBucket[bi] += jobs[jobId].ops;
                if (cpuBucket[bi] > cpuBucket[worstBucket]) worstBucket = bi;
                bi = (bi + 1) % machines; // rotate bucked
            }
            worstSize += cpuBucket[worstBucket];
        }
        return worstSize;
    }

    /**
     *
     */
    private List<List<Integer>> evalPriorityMatrix(Job[] jobs) {
        // first jobs with dependencies
        Arrays.sort(jobs, (a, b) -> Integer.compare(b.dependencies.length, a.dependencies.length));

        //
        int[] visitedPriority = new int[jobs.length];
        int maxPriority = 0;
        for (Job job : jobs) {
            // first visit only job with dependencies
            if (job.dependencies.length > 0) {
                int p = dfs(job.id, 1, visitedPriority);
                if (p > maxPriority) {
                    maxPriority = p;
                }
            }
        }
        // jobs not visited have priority 0

        // prepare priority matrix
        List<List<Integer>> pmx = new ArrayList<>();
        for (int i = 0; i < maxPriority + 1; ++i) {
            pmx.add(new ArrayList<>());
        }
        for (int i = 0; i < visitedPriority.length; ++i) {
            pmx.get(visitedPriority[i]).add(i);
        }

        // sort priority levels by size
        // I prefer to schedule the larger job first because it is more easy
        // to fill the gaps between machines
        pmx.forEach(list -> list.sort((a, b) -> Double.compare(jobs[b].ops, jobs[a].ops)));

        return pmx;
    }

    /**
     *
     */
    private int dfs(int jobId, int priority, int[] visitedPriority) {

        if (visitedPriority[jobId] >= priority) {
            return priority;
        }
        // visit the node, or revisit it and riassign priorities
        visitedPriority[jobId] = priority;

        // dependencies
        int maxPriority = priority;
        for (int d : jobs[jobId].dependencies) {
            int p = dfs(d, priority + 1, visitedPriority);
            if (p > maxPriority) {
                maxPriority = p;
            }
        }
        return maxPriority;
    }

    /**
     *
     */
    private void dumpPriorityMatrix(List<List<Integer>> priorityMatrix) {

        PrintStream pw = System.out;
        pw.println("Priority Matrix:");
        for (int i = 0; i < priorityMatrix.size(); ++i) {
            pw.printf("[%3d]: ", i);
            Iterator<Integer> it = priorityMatrix.get(i).iterator();
            if (it.hasNext()) {
                pw.print(it.next());
                while (it.hasNext()) {
                    pw.print(", ");
                    pw.print(it.next());
                }
                pw.println();
            } else {
                pw.println("-");
            }
        }
    }

    /**
     *
     */
    class SchedulePlan {

        Machine[] schedules;
        double endTime;

        public SchedulePlan(Machine[] schedules, double endTime) {
            this.schedules = schedules;
            this.endTime = endTime;
        }
    }

    /**
     *
     */
    class Machine {

        final int id;
        Queue<Schedule> schedules = new LinkedList<>();
        double endTime;

        public Machine(int id) {
            this.id = id;
        }

        void scheduleJob(Job job, int priority, int pstate) {
            Schedule s = new Schedule();
            s.job = job;
            s.priority = priority;
            s.pstate = pstate;
            s.startTime = endTime;
            s.duration = job.ops / frequencies[s.pstate];
            s.powerUsage = jobJoulePerPState[job.id][pstate];
            schedules.add(s);
            endTime += s.duration;
        }

        void sync(int priority, double endTime) {
            Schedule s = new Schedule();
            // idle to fill the gap
            s.priority = priority;
            s.startTime = endTime;
            s.duration = endTime - this.endTime;
            s.powerUsage = s.duration * powers[0];
            schedules.add(s);
            this.endTime = endTime;
        }

        void dump() {
            System.out.printf("machine %d%n", id);
            for (Schedule s : schedules) {
                if (s.job == null) { // sync
                    if (s.duration > 0) {
                        System.out.printf(" - idle    %2d p0 %12f %12f %12f%n",
                            s.priority, s.startTime, s.endTime(), s.powerUsage);
                    }
                    System.out.printf(" -------------%2d              %12f%n",
                        s.priority, s.endTime());
                } else {
                    System.out.printf(" - job:%-3d %2d p%d %12f %12f %12f%n",
                        s.job.id, s.priority, s.pstate, s.startTime, s.endTime(), s.powerUsage);
                }
            }
        }
    }

    /**
     *
     */
    class Schedule {

        Job job;
        int priority;
        int pstate;
        double startTime;
        double duration;
        double powerUsage;

        double endTime() {
            return startTime + duration;
        }
    }

    /**
     *
     */
    public static class Job {

        final int id;
        final double ops;
        final Integer[] dependencies;

        public Job(int id, double ops, Integer[] dependencies) {
            this.id = id;
            this.ops = ops;
            this.dependencies = dependencies;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(10);
            sb.append(id);
            if (dependencies.length > 0) {
                sb.append(Arrays.toString(dependencies));
            }
            return sb.toString();
        }
    }
}
