package parallelproject.phase3;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * PipeMain connects a ReaderThread and ProcessorThread via a pipe,
 * runs them concurrently, and measures the total execution time.
 *
 * Pipe architecture:
 *   ReaderThread:    PipedOutputStream -> OutputStreamWriter -> PrintWriter
 *   ProcessorThread: PipedInputStream  -> InputStreamReader  -> BufferedReader
 */
public class PipeMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        PipedOutputStream pipeOut = new PipedOutputStream();
        PipedInputStream pipeIn = new PipedInputStream();

        pipeIn.connect(pipeOut);

        String filePath = "forest_data_output.txt";

        ReaderThread readerTask = new ReaderThread(filePath, pipeOut);
        ProcessorThread processorTask = new ProcessorThread(pipeIn);

        Thread readerThread = new Thread(readerTask);
        Thread processorThread = new Thread(processorTask);

        long startTime = System.currentTimeMillis();

        readerThread.start();
        processorThread.start();

        readerThread.join();
        processorThread.join();

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Pipe execution time: " + elapsed + " ms");
    }
}
