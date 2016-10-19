package task_3.a;

import common.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class FavouriteQuestionsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        String postTypeId = Util.getAttrContent("PostTypeId", text);

        // Filter questions
        if(postTypeId.equals("1")){
            String title = Util.getAttrContent("Title", text);
            String count = Util.getAttrContent("FavoriteCount", text);

            // Check if count has a valid value (can be converted to an integer)
            if(Util.isInteger(count)){
                context.write(new Text(title), new IntWritable(Integer.parseInt(count)));
            }
        }
    }
}
