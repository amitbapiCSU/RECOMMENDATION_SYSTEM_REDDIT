package cs535.subredditrecommender.calculateratingjob;

import cs535.subredditrecommender.utilities.RatingsInformationClass;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CalculateRatingReducer extends Reducer<Text, RatingsInformationClass, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<RatingsInformationClass> values, Context context) throws IOException, InterruptedException {

        try {

            Set<String> subRedditIDSet = new HashSet<String>();
            Map<String, Double> ratingperUserandSubredditIDMap = new ConcurrentHashMap<String, Double>();
            Double ratingsperUser = 0.0;

            for (RatingsInformationClass rtnfinfo : values) {

                String subRedditID = rtnfinfo.getSubRedditID();
                Double ratingperUserandSubredditID = rtnfinfo.getRatingperUserandSubredditID();

                if ((!subRedditID.isEmpty())) {
                    if (!subRedditIDSet.contains(subRedditID)) {
                        subRedditIDSet.add(subRedditID);
                    }
                }

                if (ratingperUserandSubredditID != 0.0) {
                    ratingperUserandSubredditIDMap.put(subRedditID, ratingperUserandSubredditID);
                }


                if (ratingsperUser == 0.0) {
                    ratingsperUser = rtnfinfo.getRatingsperUser();
                }
            }
            StringBuffer sb = new StringBuffer();
            for (String subRedditid : subRedditIDSet) {

                if (ratingperUserandSubredditIDMap.containsKey(subRedditid) && (ratingsperUser != 0.0)) {
                    Double actualRatingperUserandSubReddit = ratingperUserandSubredditIDMap.get(subRedditid) / ratingsperUser;
                    sb.append(subRedditid + "#" + String.valueOf(actualRatingperUserandSubReddit) + ",");
                }
            }
            if (sb.length() != 0) {
                context.write(key, new Text(sb.toString()));
            }

            subRedditIDSet.clear();
            ratingperUserandSubredditIDMap.clear();

        } catch (Exception e) {

        }

    }
}