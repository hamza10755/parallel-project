package parallelproject.phase3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.PipedOutputStream;

/**
 * ReaderThread reads the dataset file line by line (skipping the header)
 * and sends each line through a piped output stream to the ProcessorThread.
 *
 * Pipe chain: PipedOutputStream -> OutputStreamWriter -> PrintWriter
 */
public class ReaderThread implements Runnable {

    private String filePath;
    private PipedOutputStream outputStream;

    public ReaderThread(String filePath, PipedOutputStream outputStream) {
        this.filePath = filePath;
        this.outputStream = outputStream;
    }

    @Override
    public void run() {
        try (BufferedReader fileReader = new BufferedReader(new FileReader(filePath));
             PrintWriter pipeWriter = new PrintWriter(new OutputStreamWriter(outputStream))) {

            fileReader.readLine(); // skip header row

            String line;
            while ((line = fileReader.readLine()) != null) {
                pipeWriter.println(line);
            }

        } catch (IOException ex) {
            System.getLogger(ReaderThread.class.getName())
                  .log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }
}
