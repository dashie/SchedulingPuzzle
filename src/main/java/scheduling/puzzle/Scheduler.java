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
        MachineSchedule machineSchedule = scheduleJobs(defaultPState);
        System.out.println();

        for (Machine s : machineSchedule.schedules) {
            s.dump();
        }

        //
        System.out.println();
        System.out.printf("end time: %f%n", machineSchedule.endTime);
    }

    /**
     *
     */
    private int evalPState(double timeOffset, double maxOpsCount) {
        int defaultPState = 0;
        for (int i = 1; i < frequencies.length; ++i) {
            if (timeOffset + maxOpsCount / frequencies[i] < deadline) {
                defaultPState = i;
                break;
            }
        }
        return defaultPState;
    }

    /**
     *
     */
    private MachineSchedule scheduleJobs(int defaultPState) {
        // init Machines
        Machine[] machines = new Machine[this.machines];
        for (int i = 0; i < machines.length; ++i) {
            machines[i] = new Machine(i);
        }

        //
        int mostLoadedMachine = 0;
        for (int p = priorityMatrix.size() - 1; p >= 0; --p) {

            List<Integer> list = priorityMatrix.get(p); // jobs are sorted by sizes
            if (list.size() == 0) continue;

            // organize all level jobs per machine assigning the new one to the most free machine
            int mi = 0;
            for (int jobId : list) {
                while (machines[mi].endTime > machines[nextMachine(mi, 0)].endTime) mi = nextMachine(mi, 0);
                machines[mi].scheduleJob(jobs[jobId], p, defaultPState);
                if (machines[mi].endTime > machines[mostLoadedMachine].endTime) mostLoadedMachine = mi;
                mi = nextMachine(mi, 0); // rotate bucked
            }

            //
            double currentEndTime = machines[mostLoadedMachine].endTime;

            // fill the gaps with items from 0 priority (starting from the end of the queue)
            for (Machine machine : machines) {
                if (machine != machines[mostLoadedMachine]) {
                    double dt = currentEndTime - machine.endTime;
                    while (fillMachine(machine, dt, defaultPState)) ; // fill the machine
                }
            }

            // TODO try to stretch machines less loaded to fill the gaps

            //

            // align end time on all machine
            for (Machine s : machines) {
                s.sync(currentEndTime);
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
        return new MachineSchedule(machines, machines[mostLoadedMachine].endTime);
    }

    /**
     *
     */
    private boolean fillMachine(Machine machine, double dt, int pstate) {
        if (priorityMatrix.get(0).size() > 0) {
            Integer jobId = priorityMatrix.get(0).getLast();
            if (jobTimePerPState[jobId][pstate] <= dt) {
                Job job = jobs[jobId];
                // try to reduce pstate
                while (pstate > 1 && jobTimePerPState[jobId][pstate - 1] <= dt) pstate--;
                machine.scheduleJob(job, 0, pstate);
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

        // prepare priority matrix
        List<List<Integer>> pmx = new ArrayList<>();
        pmx.add(new ArrayList<>()); // 0 priority list (jobs not related to deps logic)

        //
        boolean[] visiteds = new boolean[jobs.length];
        for (Job job : jobs) {
            if (job.dependencies.length > 0) {
                dfs(job.id, 1, pmx, visiteds);
            } else {
                if (visiteds[job.id]) continue;
                visiteds[job.id] = true;
                pmx.get(0).add(job.id);
            }
        }

        // sort priority levels by size
        pmx.forEach(list -> list.sort((a, b) -> Double.compare(jobs[b].ops, jobs[a].ops)));

        return pmx;
    }

    /**
     *
     */
    private void dfs(int jobId, int priority, List<List<Integer>> pmx, boolean[] visiteds) {

        if (visiteds[jobId]) return;
        visiteds[jobId] = true;

        // add job to priority queue
        if (pmx.size() <= priority) {
            pmx.add(new ArrayList<>());
        }
        List<Integer> priorityList = pmx.get(priority);
        priorityList.add(jobId);

        // dependencies
        for (int d : jobs[jobId].dependencies) {
            dfs(d, priority + 1, pmx, visiteds);
        }
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
    class MachineSchedule {

        Machine[] schedules;
        double endTime;

        public MachineSchedule(Machine[] schedules, double endTime) {
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
            schedules.add(s);
            endTime += s.duration;
        }

        void sync(double endTime) {
            this.endTime = endTime;
            Schedule s = new Schedule();
            s.startTime = endTime;
            schedules.add(s);
        }

        void dump() {
            System.out.printf("machine %d%n", id);
            for (Schedule s : schedules) {
                if (s.job == null) { // sync
                    System.out.printf(" --------------- %12f%n", s.startTime);
                } else {
                    System.out.printf(" - job:%-3d %2d p%d %12f %12f%n", s.job.id, s.priority, s.pstate, s.startTime, s.endTime());
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
        final Set<Integer> dependencySet;

        int machine;
        int pState;
        double startTime;
        double completionTime;

        public Job(int id, double ops, Integer[] dependencies) {
            this.id = id;
            this.ops = ops;
            this.dependencies = dependencies;
            this.dependencySet = new HashSet<>(Arrays.asList(dependencies));
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
