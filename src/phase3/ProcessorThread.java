package parallelproject.phase3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;

/**
 * ProcessorThread reads lines from a piped input stream (sent by ReaderThread),
 * applies the complex filter, and prints the total matching count.
 *
 * Pipe chain: PipedInputStream -> InputStreamReader -> BufferedReader
 * Filter: Elevation > 2800, Slope < 10, Horiz_Dist_Hydro < 50
 */
public class ProcessorThread implements Runnable {

    PipedInputStream inputStream;
    int count;

    public ProcessorThread(PipedInputStream inputStream) {
        this.inputStream = inputStream;
        this.count = 0;
    }

    @Override
    public void run() {
        try (BufferedReader pipeReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = pipeReader.readLine()) != null) {
                String[] parts = line.split(",");
                double elevation = Double.parseDouble(parts[0].trim());
                double slope = Double.parseDouble(parts[2].trim());
                double hydro = Double.parseDouble(parts[3].trim());

                if (elevation > 2800 && slope < 10 && hydro < 50) {
                    count++;
                }
            }
            System.out.println("Pipe result: " + count + " matching records");
        } catch (IOException ex) {
            System.getLogger(ProcessorThread.class.getName())
                  .log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }

    public int getCount() {
        return count;
    }
}
