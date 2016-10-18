package task_1.d;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class StopwordsReader {

    private List<String> stopwords = new ArrayList<String>();

    // Package private
    StopwordsReader(final String stopwordspath){
        readFile(stopwordspath);
    }

    private void readFile(final String stopwordspath){
        BufferedReader reader = null;

        try {
            File file = new File(stopwordspath);
            reader = new BufferedReader(new FileReader(file));

            String line;
            while ((line = reader.readLine()) != null) {
                stopwords.add(line.toLowerCase());
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Package private
    List<String> getStopwords(){
        return stopwords;
    }
}
