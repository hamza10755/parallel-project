package parallelproject;

import java.util.List;

/**
 * ForestDataProcessor — a separate class that implements Runnable.
 *
 * Each instance is given a sub-list of CSV records and applies the
 * complex filter (Elevation > 2800, Slope < 10, Horiz_Dist_Hydro < 50)
 * to every line in its slice. The count is stored locally and can be
 * retrieved with getCount() after the thread finishes.
 */
public class ForestDataProcessor implements Runnable {

    private final List<String> dataSublist;
    private int count;

    public ForestDataProcessor(List<String> dataSublist) {
        this.dataSublist = dataSublist;
        this.count = 0;
    }

    @Override
    public void run() {
        for (String record : dataSublist) {
            // Split the comma-separated values
            String[] parts = record.split(",");

            // Parse the three columns we care about
            double elevation = Double.parseDouble(parts[0].trim());
            double slope = Double.parseDouble(parts[2].trim());
            double hydroDist = Double.parseDouble(parts[3].trim());

            // Apply the multi-condition filter
            if (elevation > 2800 && slope < 10 && hydroDist < 50) {
                count++;
            }
        }
    }

    /**
     * Returns the number of records in this thread's sub-list that
     * passed the filter. Should only be called after the thread has
     * finished (i.e. after join() returns).
     */
    public int getCount() {
        return count;
    }
}
