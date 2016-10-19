package task_2.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class TopDBAsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private int[] top10DBArep = new int[10];
    private String[] top10DBAname = new String[10];
    private int smallestNumPos = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int rep = 0;
        for (IntWritable i : values) {
            rep += i.get();
        }

        // Check if reputation is higher than the smallest reputation in the top10 list.
        if (rep > top10DBArep[smallestNumPos]){
            top10DBArep[smallestNumPos] = rep;
            top10DBAname[smallestNumPos] = key.toString();

            // Find smallest number position
            for (int i = 0; i < top10DBArep.length; i++) {
                if (top10DBArep[i] < top10DBArep[smallestNumPos]){
                    smallestNumPos = i;
                }
            }
        }

        //context.write(key, new IntWritable(rep));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Sort by reputation, descending
        for (int i = 0; i < top10DBArep.length; i++){
            for (int j = 0; j < top10DBArep.length; j++){
                if(top10DBArep[i]>top10DBArep[j]){
                    // sort reputation array
                    int tempVal = top10DBArep[j];
                    top10DBArep[j] = top10DBArep[i];
                    top10DBArep[i] = tempVal;

                    // sort name array
                    String tempName = top10DBAname[j];
                    top10DBAname[j] = top10DBAname[i];
                    top10DBAname[i] = tempName;
                }
            }
        }

        // Generate string output
        String summary = "";
        for (int i = 0; i < top10DBArep.length; i++) {
            summary += (top10DBArep[i]+":\t"+top10DBAname[i]+"\n");
        }
        context.write(new Text(summary), NullWritable.get());
    }
}
