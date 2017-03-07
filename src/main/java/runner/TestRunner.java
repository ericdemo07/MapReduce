package runner;


import org.apache.hadoop.mapred.JobPriority;

import java.util.HashMap;

/**
 * Created by ayush.shukla on 3/3/2017.
 */
public class TestRunner {
    public static void main(String[] args) throws Exception {

        String[] customArgs = new String[9];

        customArgs[0] = "test/";  //input path
        customArgs[1] = "Output1";                                              //first output
        customArgs[2] = "mapper.TestMapper";                                    //first Mapper
        customArgs[3] = "reducer.TestReducer";                                  //first reducer
        customArgs[4] = "Y";                                                    //additional mapper flag
        customArgs[5] = "Output1";                                              //second input
        customArgs[6] = "Output2";                                              //second output
        customArgs[7] = "mapper.TestMapper1";                                   //second mapper
        customArgs[8] = "reducer.TestReducer1";                                 //second reducer

        HashMap<String, String> map = new HashMap<>();                         //config map
        map.put("mapred.job.priority", JobPriority.VERY_HIGH.toString());

        CustomJob customJob = new CustomJob();
        customJob.execute(customArgs, map);
    }
}
