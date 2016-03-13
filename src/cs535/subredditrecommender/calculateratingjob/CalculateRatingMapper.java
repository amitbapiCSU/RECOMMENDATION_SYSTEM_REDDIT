package cs535.subredditrecommender.calculateratingjob;


import cs535.subredditrecommender.utilities.RatingsInformationClass;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CalculateRatingMapper extends Mapper<LongWritable, Text, Text, RatingsInformationClass> {

    private RatingsInformationClass ratingInfo = new RatingsInformationClass();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            String input_line = value.toString().trim();
            String[] linearr = input_line.split("\t");

            String keyInfo = "";

            if (linearr[0].contains("#")) {
                String[] arr = linearr[0].split("#");
                keyInfo = arr[0];

                ratingInfo.setSubRedditID(arr[1]);
                ratingInfo.setRatingsperUserandSubredditID(Double.parseDouble(linearr[1]));
                ratingInfo.setRatingsperUser(0.0);

            } else {
                keyInfo = linearr[0];
//				String[] arr1 = linearr[1].split("#");

                ratingInfo.setSubRedditID("");
                ratingInfo.setRatingsperUserandSubredditID(0.0);
                ratingInfo.setRatingsperUser(Double.parseDouble(linearr[1])); // arr1
            }

            context.write(new Text(keyInfo), ratingInfo);

        } catch (Exception e) {

        }

    }


}
