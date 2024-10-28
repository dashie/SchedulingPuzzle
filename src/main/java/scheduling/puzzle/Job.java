package scheduling.puzzle;

import java.util.Arrays;

/**
 *
 */
public class Job {

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
