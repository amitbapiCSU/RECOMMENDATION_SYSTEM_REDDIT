package cs535.subredditrecommender.partitiondatasetjob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionDataSetReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {

            for (Text val : values) {
                context.write(val, new Text(""));
            }

        } catch (Exception e) {

        }

    }

}
