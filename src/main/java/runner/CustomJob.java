package runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class CustomJob extends Configured implements Tool {

    private static Map<String, String> configMap = new HashMap();
    public void execute(String[] customArgs, Map maps) {
        try {
            configMap = maps;
            ToolRunner.run(new Configuration(), new CustomJob(), customArgs);
            if (customArgs[4].equalsIgnoreCase("Y")) {
                String[] customArgsForNextMapper = Arrays.copyOfRange(customArgs, customArgs.length - 4, customArgs.length);
                ToolRunner.run(new Configuration(), new CustomJob(), customArgsForNextMapper);
            }
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        configMap.forEach(conf::set);

        Job job = new Job(conf);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("testJob");
        job.setJarByClass(Class.forName("runner.CustomJob"));
        job.setMapperClass((Class<? extends Mapper>) Class.forName(args[2]));
        if (args[3] != null && !args[3].equalsIgnoreCase("N"))
            job.setReducerClass((Class<? extends Reducer>) Class.forName(args[3]));
        //job.setCombinerClass(TestCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);

        //System.exit(0);
        return success ? 0 : 1;
    }
}
