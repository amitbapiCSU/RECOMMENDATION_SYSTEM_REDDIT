package cs535.subredditrecommender.calculatesubredditweightjob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CalculateSubredditWeightMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            String input_line = value.toString().trim();

            String[] linearr = input_line.split("\t");

            String subRedditIDs = linearr[1];
            String[] subRedditIDarr = subRedditIDs.split(",");
            for (String subRedditRtngID : subRedditIDarr) {
                String[] subRedditRtngIDarr = subRedditRtngID.split("#");
                String subRedditID = subRedditRtngIDarr[0];
                String ratings = subRedditRtngIDarr[1];
                context.write(new Text(subRedditID), new Text(ratings));
            }

        } catch (Exception e) {

        }

    }
}
