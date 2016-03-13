package cs535.subredditrecommender.calculatesubredditweightjob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalculateSubredditWeightReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {

            Double totalRatingsSumperSubReddit = 0.0;

            for (Text val : values) {
                Double ratings = Double.parseDouble(val.toString());
                totalRatingsSumperSubReddit += ratings;
            }

            Double logtotalRatingWeight = Math.log(totalRatingsSumperSubReddit);

            context.write(key, new Text(String.valueOf(logtotalRatingWeight)));

        } catch (Exception e) {

        }

    }
}
