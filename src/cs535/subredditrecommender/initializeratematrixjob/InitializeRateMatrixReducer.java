package cs535.subredditrecommender.initializeratematrixjob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InitializeRateMatrixReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {

            Double totalRatingsperUser = 0.0;
            String RatingperUserandSubRedditID = "0.0";
//            String subRedditID = "";

            for (Text val : values) {
                RatingperUserandSubRedditID = val.toString();
                String[] subredditRatings = RatingperUserandSubRedditID.split("#");
                totalRatingsperUser += Double.parseDouble(subredditRatings[1].trim());
                context.write(new Text(key.toString() + "#" + subredditRatings[0].trim()), new Text(subredditRatings[1].trim()));
            }

            if (totalRatingsperUser != 0.0D) {
                context.write(key, new Text(String.valueOf(totalRatingsperUser)));
            }

        } catch (Exception e) {

        }

    }

}
