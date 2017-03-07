package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ayush.shukla on 3/3/2017.
 */
public class TestReducer extends Reducer<Text, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(""));
    }
}

