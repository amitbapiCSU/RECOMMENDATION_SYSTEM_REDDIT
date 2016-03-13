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

public class doRecommendandEvaluateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static String DELIM = "|"; // ROHIT WILL TELL
    private Map<String, Map<String, Double>> centroiduserSubRedditRatingsMap = new HashMap<String, Map<String, Double>>();
    private Map<String, String> centroidUserClusterMembersMap = new HashMap<String, String>();

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        Path clusteropFile = new Path(conf.get("KMEANS_CLUSTER_OUTPUT"));
        FSDataInputStream fs = dfs.open(clusteropFile);
        BufferedReader flReader = new BufferedReader(new InputStreamReader(fs));

        String cur = flReader.readLine();
        while (cur != null) {

            String centroidUser = (cur.split(DELIM))[0];
            String[] centroidUserarr = centroidUser.split("\\t");
            String[] centroidUserSubRedditRatingsarr = centroidUserarr[1].split(",");

            for (String centroidUserSubRedditRating : centroidUserSubRedditRatingsarr) {
                String[] subredditRtngsArr = centroidUserSubRedditRating.split("#");
                Map<String, Double> subrdtRatingsMap = new HashMap<String, Double>();
                subrdtRatingsMap.put(subredditRtngsArr[0], Double.parseDouble(subredditRtngsArr[1]));
                centroiduserSubRedditRatingsMap.put(centroidUserarr[0], subrdtRatingsMap);
            }

            centroidUserClusterMembersMap.put(centroidUser, cur);
            cur = flReader.readLine();
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            String inputLine = value.toString().trim();
            String[] clusterMembers = inputLine.split(DELIM);
            for (int i = 1; i < clusterMembers.length; i++) {
                String clusterMember = clusterMembers[i];

                String[] arr = clusterMember.split("\\t");
                String userID = arr[0];
                String subRedditswithRatings = arr[1];
                String closestCentroid = "";
                Double maxsimilarity = -9E9D;
                Map<String, Double> testuserSubRedditRatingsMap = new HashMap<String, Double>();

                // Populate TEST user's  SubRedditRatingsMap
                String[] subRedditswithRatingsarr = subRedditswithRatings.split(",");
                Double partnCoeff = 0.6;
                int trnprtnLen = 1;
                if (subRedditswithRatingsarr.length > 1) {
                    trnprtnLen = (int) (partnCoeff * subRedditswithRatingsarr.length);
                }

                int itrtncnt = 0;
//                String trainsubRedditRatingstr = "";
//                String testSubRedditRatingstr = "";
                StringBuilder trainsubRedditRatingstr = new StringBuilder();
                StringBuilder testSubRedditRatingstr = new StringBuilder();

                for (String subRdtwidRtng : subRedditswithRatingsarr) {
                    String[] tmparr = subRdtwidRtng.split("#");

                    if (itrtncnt <= trnprtnLen) {
//                        trainsubRedditRatingstr += subRdtwidRtng+",";
                        trainsubRedditRatingstr.append(subRdtwidRtng);
                        trainsubRedditRatingstr.append(",");
                        testuserSubRedditRatingsMap.put(tmparr[0], Double.parseDouble(tmparr[1]));
                    } else {
//                        testSubRedditRatingstr += subRdtwidRtng+",";
                        testSubRedditRatingstr.append(subRdtwidRtng);
                        testSubRedditRatingstr.append(",");
                    }
                    itrtncnt++;
                }


                for (String centroid : centroiduserSubRedditRatingsMap.keySet()) {
                    Double similarity = 0.0;
                    if (closestCentroid.isEmpty()) {
                        closestCentroid = centroid;
                    }
                    for (String testUserSubReddit : testuserSubRedditRatingsMap.keySet()) {
                        if (centroiduserSubRedditRatingsMap.get(centroid).containsKey(testUserSubReddit)) {
                            similarity += (testuserSubRedditRatingsMap.get(testUserSubReddit)) * (centroiduserSubRedditRatingsMap.get(centroid).get(testUserSubReddit));
                        }
                    }

                    if (similarity > maxsimilarity) {
                        maxsimilarity = similarity;
                        closestCentroid = centroid;
                    }
                }

                if (!closestCentroid.isEmpty()) {
                    context.write(new Text(userID + "|" + trainsubRedditRatingstr + "|" + testSubRedditRatingstr), new Text(centroidUserClusterMembersMap.get(closestCentroid)));
                }

                testuserSubRedditRatingsMap.clear();
                centroiduserSubRedditRatingsMap.clear();

            }


        } catch (Exception e) {

        }

    }
}
