package cs535.subredditrecommender.generic;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TabSeparatedMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyValue = value.toString().split("\\t");
        context.write(new Text(keyValue[0]), new Text(keyValue[1]));
    }
}
