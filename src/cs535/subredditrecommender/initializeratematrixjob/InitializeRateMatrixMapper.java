package cs535.subredditrecommender.initializeratematrixjob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InitializeRateMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {

            String input_line = value.toString().trim();

            String[] linearr = input_line.split("\t");
            String[] infoarr = linearr[1].split(",");

            String userID = linearr[0];
            String subRedditID = infoarr[0];
            Double nofComments = Double.parseDouble(infoarr[1]);
            Double score_upvotes = Double.parseDouble(infoarr[2]);

            Double rtng = score_upvotes / nofComments;

            context.write(new Text(userID), new Text(subRedditID + "#" + rtng.toString()));


        } catch (Exception e) {

        }

    }

}
