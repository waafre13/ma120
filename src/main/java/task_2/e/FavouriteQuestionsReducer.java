package task_2.e;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class FavouriteQuestionsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private int[] top10Score = new int[10];
    private String[] top10Name = new String[10];
    private int smallestNumPos = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int rep = 0;
        for (IntWritable i : values) {
            rep += i.get();
        }

        // Check if reputation is higher than the smallest reputation in the top10 list.
        if (rep > top10Score[smallestNumPos]){
            top10Score[smallestNumPos] = rep;
            top10Name[smallestNumPos] = key.toString();

            // Find smallest number position
            for (int i = 0; i < top10Score.length; i++) {
                if (top10Score[i] < top10Score[smallestNumPos]){
                    smallestNumPos = i;
                }
            }
        }

        //context.write(key, new IntWritable(rep));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Sort by reputation, descending
        for (int i = 0; i < top10Score.length; i++){
            for (int j = 0; j < top10Score.length; j++){
                if(top10Score[i]> top10Score[j]){
                    // sort reputation array
                    int tempVal = top10Score[j];
                    top10Score[j] = top10Score[i];
                    top10Score[i] = tempVal;

                    // sort name array
                    String tempName = top10Name[j];
                    top10Name[j] = top10Name[i];
                    top10Name[i] = tempName;
                }
            }
        }

        // Generate string output
        String summary = "";
        for (int i = 0; i < top10Score.length; i++) {
            summary += (top10Score[i]+":\t"+ top10Name[i]+"\n");
        }
        context.write(new Text(summary), NullWritable.get());
    }
}
