package parallelproject.phase3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * CallableMain reads the dataset, then tests the complex filter using
 * a thread pool with 2, 4, 8, 16, 32, 64, and 128 threads.
 *
 * The Callable is defined inline as a lambda expression — no separate
 * class file is needed.
 */
public class CallableMain {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        // --- Load the dataset ---
        List<String> allLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("forest_data_output.txt"))) {
            reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                allLines.add(line);
            }
        }
        System.out.println("Loaded " + allLines.size() + " records.\n");

        // --- Thread counts to test ---
        int[] threadCounts = {2, 4, 8, 16, 32, 64, 128};

        for (int numThreads : threadCounts) {
            long startTime = System.currentTimeMillis();

            int result = runWithThreadPool(allLines, numThreads);

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("Threads=" + numThreads + " | Count=" + result
                    + " | Time=" + elapsed + " ms");
        }
    }

    /**
     * Splits the data into numThreads chunks and uses a thread pool to
     * process each chunk in parallel. Each chunk's Callable is defined
     * inline as a lambda expression.
     */
    private static int runWithThreadPool(List<String> data, int numThreads)
            throws InterruptedException, ExecutionException {

        int size = data.size();
        int chunk = (int) Math.ceil((double) size / numThreads);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Integer>> futures = new ArrayList<>();

        // Divide the dataset evenly and submit a lambda Callable for each slice
        for (int i = 0; i < numThreads; i++) {
            int start = i * chunk;
            int end = Math.min(start + chunk, size);
            List<String> sublist = data.subList(start, end);

            // Lambda expression implementing Callable<Integer>
            Callable<Integer> callableTask = (() -> {
                int count = 0;
                for (String record : sublist) {
                    String[] parts = record.split(",");
                    double elevation = Double.parseDouble(parts[0].trim());
                    double slope = Double.parseDouble(parts[2].trim());
                    double hydro = Double.parseDouble(parts[3].trim());

                    if (elevation > 2800 && slope < 10 && hydro < 50) {
                        count++;
                    }
                }
                return count;
            });

            Future<Integer> future = executor.submit(callableTask);
            futures.add(future);
        }

        // Collect partial results from each Future
        int total = 0;
        for (Future<Integer> f : futures) {
            total += f.get();
        }

        executor.shutdown();
        return total;
    }
}
