package test.homework.hadoop;

import homework.hadoop.LongestWordDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;


public class LongestWordDriverJobTest {

    @Test
    @Ignore // doesn't work on Windows
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path(INPUT);
        Path output = new Path(OUTPUT);

        LongestWordDriver driver = new LongestWordDriver();
        driver.setConf(conf);

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        int exitCode = driver.run(new String[] {input.toString(), output.toString()});
        assertThat(exitCode)
                .isEqualTo(0);

        checkOutput();
    }

    private void checkOutput() throws IOException{
        java.nio.file.Path file = Paths.get(OUTPUT + File.separator + OUTPUT_FILE_NAME);
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            int amountOfLines  = 0;
            while ((line = reader.readLine()) != null) {
                verifyLine(line);
                amountOfLines++;
            }
            verifyAmountOfLines(amountOfLines);
        }
    }

    private static void verifyLine(String line) {
        assertThat(in(line, EXPECTED_WORDS));
    }

    private static boolean in(String value, String... inArray) {
        for (String currentIn : inArray) {
            if (currentIn.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private void verifyAmountOfLines(int amountOfLines) {
        assertThat(amountOfLines)
                .isEqualTo(EXPECTED_AMOUNT_OF_LINES)
                .withFailMessage("Only %s lines in the file are expected. Actual amount: %s",
                                 EXPECTED_AMOUNT_OF_LINES,
                                 amountOfLines);
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String OUTPUT_FILE_NAME = "part-r-00000";
    private static final int EXPECTED_AMOUNT_OF_LINES = 3;
    private static final String[] EXPECTED_WORDS = {"three", "seven", "eight"};
}
