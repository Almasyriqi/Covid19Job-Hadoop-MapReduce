package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
public class App 
{
    public static void main( String[] args )
    {
        JobConf conf = new JobConf(App.class);
        conf.setJobName("Covid19MapReduce");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(CovidMapper.class);
        conf.setCombinerClass(CovidReducer.class);
        conf.setReducerClass(CovidReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            JobClient.runJob(conf);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
