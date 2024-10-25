package scheduling.puzzle;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Integer.valueOf;

/**
 *
 */
public class Launcher {

    public static void main(String[] args) throws IOException {
        new Launcher().run("./samples/in-competition.dat");
    }

    /**
     *
     */
    private List<Scheduler> schedulers = new ArrayList<>();

    /**
     *
     */
    private void run(String filePath) throws IOException {
        loadInput(filePath);
        schedulers.forEach(s -> {
            s.prepare();
            s.solve();
        });
        System.out.println("END");
    }

    /**
     *
     */
    private void loadInput(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(filePath)))) {

            int numberOfScheduling = parseInt(reader.readLine());
            while (numberOfScheduling-- > 0) {

                reader.readLine(); // skip line

                //
                int numberOfJobs = parseInt(reader.readLine());
                double[] jobOps = parseArrayOfDouble(reader.readLine());
                int machines = parseInt(reader.readLine());
                int pStates = parseInt(reader.readLine());
                double[] frequencies = parseArrayOfDouble(reader.readLine());
                double[] powers = parseArrayOfDouble(reader.readLine());
                Scheduler.Job[] jobs = new Scheduler.Job[numberOfJobs];
                for (int i = 0; i < numberOfJobs; ++i) {
                    Integer[] predecessors = parseArrayOfInt(reader.readLine(), -1);
                    if (predecessors.length == 1 && predecessors[0] == -1) {
                        jobs[i] = new Scheduler.Job(i, jobOps[i], new Integer[0]);
                    } else {
                        jobs[i] = new Scheduler.Job(i, jobOps[i], predecessors);
                    }
                }
                double deadline = parseDouble(reader.readLine());

                //
                Scheduler s = new Scheduler(
                    numberOfJobs,
                    machines,
                    pStates,
                    frequencies,
                    powers,
                    jobs,
                    deadline
                );
                schedulers.add(s);
            }
        }
    }

    /**
     *
     */
    private double[] parseArrayOfDouble(String str) {
        return Arrays.stream(str.split(" "))
                     .mapToDouble(s -> parseDouble(s))
                     .toArray();
    }

    /**
     * Parse a string of integers (separated by space) and modify them
     * adding the {@code delta} parameter.
     */
    private Integer[] parseArrayOfInt(String str, int delta) {
        return Arrays.stream(str.split(" "))
                     .map(s -> valueOf(s) + delta)
                     .toArray(Integer[]::new);
    }

}
