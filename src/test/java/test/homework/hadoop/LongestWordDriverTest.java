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
import java.util.StringTokenizer;

import static org.assertj.core.api.Assertions.assertThat;


public class LongestWordDriverTest {

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
                checkLine(line);
                amountOfLines++;
                System.out.println(line);
            }
            assertThat(amountOfLines)
                    .isEqualTo(EXPECTED_AMOUNT_OF_LINES)
                    .withFailMessage("Only %s lines in the file are expected. Actual amount: %s",
                                     EXPECTED_AMOUNT_OF_LINES,
                                     amountOfLines);
        }
    }

    private static boolean in(String value, String... inArray) {
        for (String currentIn : inArray) {
            if (currentIn.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private static void checkLine(String line) {
        StringTokenizer itr = new StringTokenizer(line, "\t");
        assertThat(itr.hasMoreTokens()).withFailMessage("The line format is not correct. Line: %s", line);
        String key = itr.nextToken();
        assertThat(itr.hasMoreTokens()).withFailMessage("The line format is not correct. Line: %s", line);
        String value = itr.nextToken();
        assertThat(!itr.hasMoreTokens()).withFailMessage("The line format is not correct. Line: %s", line);
        assertThat(key).isEqualTo(EXPECTED_MAX_LENGTH);
        assertThat(in(value, EXPECTED_WORDS));
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String OUTPUT_FILE_NAME = "part-r-00000";
    private static final int EXPECTED_AMOUNT_OF_LINES = 3;
    private static final String EXPECTED_MAX_LENGTH = "5";
    private static final String[] EXPECTED_WORDS = {"three", "seven", "eight"};
}
