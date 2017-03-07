package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ayush.shukla on 3/3/2017.
 */
public class TestMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            context.write(new Text("ayush"), new Text());
            context.write(new Text("shukla"), new Text("ssaa"));
            context.write(new Text("sam"), new Text("ssaa"));
            context.write(new Text("breganza"), new Text("ssaa"));
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
