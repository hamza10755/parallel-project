package parallelproject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParallelProject {

    public static void main(String[] args) throws IOException {
        BufferedReader reader = null;
        try {
            System.out.println("Step 1: Loading Data");
            List<String> allLines = new ArrayList<>();
            reader = new BufferedReader(new FileReader("forest_data_output.txt"));
            reader.readLine(); // skip the header
            String line;
            while ((line = reader.readLine()) != null) {
                allLines.add(line);
            }
            int total = allLines.size();
            System.out.println("Loaded " + total + " records\n");

            System.out.println("Step 2: Sequential Processing");
            long start = System.currentTimeMillis();
            System.out.println("Simple Count: " + total
                    + "  | Time: " + (System.currentTimeMillis() - start) + " ms");
            //Complex filter: Elevation > 2800 AND Slope < 10 AND Horiz_Dist_Hydro < 50
            start = System.currentTimeMillis();
            int seqCount = 0;
            for (String record : allLines) {
                if (passesFilter(record)) {
                    seqCount++;
                }
            }   System.out.println("Complex Count: " + seqCount
                    + "  | Time: " + (System.currentTimeMillis() - start) + " ms\n");

            System.out.println("Step 3: Parallel Outer Threading");
            int[] threadCounts = {2, 4, 8, 16, 32, 64, 128};
            for (int numThreads : threadCounts) {
                start = System.currentTimeMillis();
                // Decompose the data and run with the separate Runnable class
                int result = runWithOuterThreads(allLines, numThreads);
                System.out.println("Threads=" + numThreads + " | Count=" + result
                        + " | Time=" + (System.currentTimeMillis() - start) + " ms");
            }   System.out.println();

            System.out.println("Step 4: Parallel Inner Threading");
            for (int numThreads : threadCounts) {
                start = System.currentTimeMillis();
                int result = runWithInnerThreads(allLines, numThreads);
                System.out.println("Threads=" + numThreads + " | Count=" + result
                        + " | Time=" + (System.currentTimeMillis() - start) + " ms");
            }
        } catch (FileNotFoundException ex) {
            System.getLogger(ParallelProject.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
            try {
                reader.close();
            } catch (IOException ex) {
                System.getLogger(ParallelProject.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
            }
        
    }

    /**
     * Returns true if the record passes the complex filter:
     * Elevation > 2800, Slope < 10, Horiz_Dist_Hydro < 50.
     */
    private static boolean passesFilter(String record) {
        String[] parts = record.split(",");
        double elevation = Double.parseDouble(parts[0].trim());
        double slope = Double.parseDouble(parts[2].trim());
        double hydroDist = Double.parseDouble(parts[3].trim());
        return elevation > 2800 && slope < 10 && hydroDist < 50;
    }

    /**
     * Outer threading: splits the full list into numThreads chunks,
     * creates a ForestDataProcessor for each, starts them, joins them,
     * then sums the partial results.
     */
    private static int runWithOuterThreads(List<String> data, int numThreads) {
        int size = data.size();
        // ceil division so no chunk is left over-sized
        int chunk = (int) Math.ceil((double) size / numThreads);

        ForestDataProcessor[] workers = new ForestDataProcessor[numThreads];
        Thread[] threads = new Thread[numThreads];

        // Divide the dataset evenly: each worker gets a subList view
        for (int i = 0; i < numThreads; i++) {
            int start = i * chunk;
            int end = Math.min(start + chunk, size);
            workers[i] = new ForestDataProcessor(data.subList(start, end));
            threads[i] = new Thread(workers[i]);
        }

        // Fire all threads
        for (Thread t : threads) {
            t.start();
        }

        // Wait for every thread to finish (join blocks the main thread)
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                // restore interrupt flag and carry on
                Thread.currentThread().interrupt();
            }
        }

        // Collect results from each worker
        int total = 0;
        for (ForestDataProcessor w : workers) {
            total += w.getCount();
        }
        return total;
    }

    /**
     * Inner threading: same data decomposition, but the Runnable logic
     * is written as an anonymous inner class inside the loop.
     * Each thread writes its partial count to a dedicated index in a
     * shared int[] — no two threads use the same index, so it is safe
     * without synchronization.
     */
    private static int runWithInnerThreads(List<String> data, int numThreads) {
        int size = data.size();
        int chunk = (int) Math.ceil((double) size / numThreads);

        int[] results = new int[numThreads];
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int start = i * chunk;
            final int end = Math.min(start + chunk, size);
            final int index = i;

            // Anonymous inner Runnable — no separate class file needed
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    int count = 0;
                    for (int j = start; j < end; j++) {
                        String[] parts = data.get(j).split(",");
                        double elevation = Double.parseDouble(parts[0].trim());
                        double slope = Double.parseDouble(parts[2].trim());
                        double hydro = Double.parseDouble(parts[3].trim());
                        if (elevation > 2800 && slope < 10 && hydro < 50) {
                            count++;
                        }
                    }
                    results[index] = count;
                }
            });
        }

        // Start then join every thread
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Sum the partial counts
        int total = 0;
        for (int c : results) {
            total += c;
        }
        return total;
    }
}
