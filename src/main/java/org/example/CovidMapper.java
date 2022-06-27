package org.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import static java.lang.String.format;
public class CovidMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text country_name = new Text();
    private Text data = new Text();
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String line = value.toString();

        // Pecah dulu berdasarkan ','
        String[] split = line.split(",");
        System.out.println("Jumlah Split: " + split.length);

        // cek apakah terdapat data case dan death
        if(split.length > 7) {
            String country = split[2];
            country = country.trim();
            System.out.println("Country name : " + country);

            // Cari data views, like, dislike, comment
            String cases = split[5];
            String death = split[7];

            // Laporkan ke kolektor
            this.country_name.set(format("%s,",country));
            this.data.set(format("%s,%s", cases, death));
            output.collect(this.country_name, this.data);
        }
    }
}
