package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ayush.shukla on 3/3/2017.
 */
public class TestMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private static int count = 0;

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            count++;
            context.write(new Text("same"), new Text(String.valueOf(count)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
