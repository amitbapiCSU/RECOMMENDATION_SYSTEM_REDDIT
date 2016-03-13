package cs535.subredditrecommender.dorecommendevaluatejob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class doRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

    Map<String, Map<String, Double>> centroidUserSubRedditAccumulatedWeightMap = new HashMap<String, Map<String, Double>>();

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        Path clusteropFile = new Path(conf.get("CLUSER_SUBREDDIT_ACCUMULATED_WEIGHT"));
        FSDataInputStream fs = dfs.open(clusteropFile);
        BufferedReader flReader = new BufferedReader(new InputStreamReader(fs));

        String cur = flReader.readLine();
        while (cur != null) {

            String[] centroidUserSubRedditListRatingsarr = cur.split("\\t");
            String ClusterCenterUserID = centroidUserSubRedditListRatingsarr[0];
            String[] ClustersubRedditRatingsarr = centroidUserSubRedditListRatingsarr[1].split("\\,");

            for (String ClustersubRedditRating : ClustersubRedditRatingsarr) {

                String[] subRedditRatingarr = ClustersubRedditRating.split("\\#");

                if (centroidUserSubRedditAccumulatedWeightMap.containsKey(ClusterCenterUserID)) {

                    Map<String, Double> subRedditAccumWeightMap = centroidUserSubRedditAccumulatedWeightMap.get(ClusterCenterUserID);
                    subRedditAccumWeightMap.put(subRedditRatingarr[0], Double.parseDouble(subRedditRatingarr[1]));
                    centroidUserSubRedditAccumulatedWeightMap.put(ClusterCenterUserID, subRedditAccumWeightMap);

                } else {
                    Map<String, Double> subRedditAccumWeightMap = new HashMap<String, Double>();
                    subRedditAccumWeightMap.put(subRedditRatingarr[0], Double.parseDouble(subRedditRatingarr[1]));
                    centroidUserSubRedditAccumulatedWeightMap.put(ClusterCenterUserID, subRedditAccumWeightMap);
                }

            }
            cur = flReader.readLine();
        }
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            String inputLine = value.toString().trim();
            String[] inputLinearr = inputLine.split("\\t");
            String testUserID = inputLinearr[0];

            String closestCenterUSERID = "";
            Double maxsimilarity = -9E9D;
            Map<String, Double> testuserSubRedditRatingsMap = new HashMap<String, Double>();

            String[] subRedditswithRatingsarr = inputLinearr[1].split("\\,");
            Double partnCoeff = 0.6;
            int trnprtnLen = 1;
            if (subRedditswithRatingsarr.length > 1) {
                trnprtnLen = (int) (partnCoeff * subRedditswithRatingsarr.length);
            }

            int itrtncnt = 0;
            StringBuilder trainsubRedditRatingstr = new StringBuilder();
            StringBuilder testSubRedditRatingstr = new StringBuilder();

            for (String subRdtwidRtng : subRedditswithRatingsarr) {
                String[] subRdtwidRtngarr = subRdtwidRtng.split("\\#");

                if (itrtncnt <= trnprtnLen) {

                    trainsubRedditRatingstr.append(subRdtwidRtngarr[0]);
                    trainsubRedditRatingstr.append(",");

                    testuserSubRedditRatingsMap.put(subRdtwidRtngarr[0], Double.parseDouble(subRdtwidRtngarr[1]));
                } else {
                    testSubRedditRatingstr.append(subRdtwidRtngarr[0]);
                    testSubRedditRatingstr.append(",");
                }

                itrtncnt++;
            }

            for (String centerUserID : centroidUserSubRedditAccumulatedWeightMap.keySet()) {
                Double similarity = 0.0D;
                for (String testUserSubReddit : testuserSubRedditRatingsMap.keySet()) {
                    if ((centroidUserSubRedditAccumulatedWeightMap.get(centerUserID)).containsKey(testUserSubReddit)) {
                        similarity += testuserSubRedditRatingsMap.get(testUserSubReddit) * (centroidUserSubRedditAccumulatedWeightMap.get(centerUserID)).get(testUserSubReddit);
                    }
                }

                if (similarity > maxsimilarity && similarity != 0.0D) {
                    maxsimilarity = similarity;
                    closestCenterUSERID = centerUserID;
                }
            }

            if (!closestCenterUSERID.isEmpty()) {

                StringBuilder recommendedSubReddits = new StringBuilder();

                int idx = 0;
                Double toEmitClosestSubReddits = (centroidUserSubRedditAccumulatedWeightMap.get(closestCenterUSERID)).size() * 0.6D;

                for (String closestCenterSubreddit : (centroidUserSubRedditAccumulatedWeightMap.get(closestCenterUSERID)).keySet()) {
                    if (!testuserSubRedditRatingsMap.containsKey(closestCenterSubreddit)) {
                        recommendedSubReddits.append(closestCenterSubreddit);
                        recommendedSubReddits.append("#");
                        recommendedSubReddits.append((centroidUserSubRedditAccumulatedWeightMap.get(closestCenterUSERID)).get(closestCenterSubreddit));
                        recommendedSubReddits.append(",");
                        idx++;
                    }
                    if(toEmitClosestSubReddits <= idx) break;
                }

                context.write(new Text(testUserID), new Text(trainsubRedditRatingstr.toString() + "%" + testSubRedditRatingstr.toString() + "$" + recommendedSubReddits.toString()));
            }

            testuserSubRedditRatingsMap.clear();

        } catch (Exception e) {

        }

    }
}
