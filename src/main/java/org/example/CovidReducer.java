package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;

public class CovidReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private Text data = new Text();

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {

        int cases = 0, death = 0;
        System.out.println(key);
        while (values.hasNext()) {
            Text value = values.next();

            // pecah berdasarkan ";"
            String hasil = value.toString();
            String[] split = hasil.split(";");
            cases = Integer.parseInt(split[0]);
            death = Integer.parseInt(split[1]);
        }

        this.data.set(format("%s;%s", cases, death));
        output.collect(key, this.data);
    }
}
