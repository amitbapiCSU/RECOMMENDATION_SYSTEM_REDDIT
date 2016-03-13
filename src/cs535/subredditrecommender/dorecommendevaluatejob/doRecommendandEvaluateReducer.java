package cs535.subredditrecommender.dorecommendevaluatejob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class doRecommendandEvaluateReducer extends Reducer<Text, Text, Text, Text> {

    private static String DELIM = "\\t";
    private Map<String, String> subRedditWeightMap = new HashMap<String, String>();

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        Path clusteropFile = new Path(conf.get("SUBREDDIT_WEIGHT_OUTPUT"));
        FSDataInputStream fs = dfs.open(clusteropFile);
        BufferedReader flReader = new BufferedReader(new InputStreamReader(fs));

        String cur = flReader.readLine();
        while (cur != null) {

            String[] tmparr = cur.split(DELIM);

            subRedditWeightMap.put(tmparr[0], tmparr[1]);

            cur = flReader.readLine();
        }
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        try {

            Map<String, List<String>> clusterUserSubRedditMap = new HashMap<String, List<String>>();
            Map<String, Map<String, Double>> clusterUserSubRedditRatingsMap = new HashMap<String, Map<String, Double>>();
            Map<String, Double> testUserTrainSubRedditRatingsMap = new HashMap<String, Double>();
            List<String> testUserTestSubRedditRatingsList = new ArrayList<String>();

            for (Text val : values) {
                String clusterMemberstr = val.toString();

                String[] clusterMembers = clusterMemberstr.split("|");


                for (int i = 1; i < clusterMembers.length; i++) { // String clusterMember : clusterMembers
                    String clusterMember = clusterMembers[i];
                    String[] clusterUserSubRedditarr = clusterMember.split("\\t");
                    String clusterUserID = clusterUserSubRedditarr[0];

                    String clustersubRedditRatings = clusterUserSubRedditarr[1];
                    String[] clustersubRedditRatingsarr = clustersubRedditRatings.split(",");
                    for (String clustersubRedditRating : clustersubRedditRatingsarr) {

                        String[] sbrdtrtngarr = clustersubRedditRating.split("#");
                        String subRedditID = sbrdtrtngarr[0];
                        Double ratings = Double.parseDouble(sbrdtrtngarr[1]);

                        // Populate User-SubReddit Map :: NO NEED
//                        if (clusterUserSubRedditMap.containsKey(clusterUserID)) {
//                            List<String> subreddits = (clusterUserSubRedditMap.get(clusterUserID));
//                            subreddits.add(subRedditID);
//                            clusterUserSubRedditMap.put(clusterUserID, subreddits);
//                        } else {
//                            List<String> subreddits = new ArrayList<String>();
//                            subreddits.add(subRedditID);
//                            clusterUserSubRedditMap.put(clusterUserID, subreddits);
//                        }

                        // Populate User-SubReddit-Ratings Map
                        if (clusterUserSubRedditRatingsMap.containsKey(clusterUserID)) {

                            Map<String, Double> subRedditRatingMap = clusterUserSubRedditRatingsMap.get(clusterUserID);
                            subRedditRatingMap.put(subRedditID, ratings);
                            clusterUserSubRedditRatingsMap.put(clusterUserID, subRedditRatingMap);

                        } else {
                            Map<String, Double> subRedditRatingMap = new HashMap<String, Double>();
                            subRedditRatingMap.put(subRedditID, ratings);
                            clusterUserSubRedditRatingsMap.put(clusterUserID, subRedditRatingMap);
                        }

                    }


                }
            }

            String testUser = key.toString();
            String[] testUserarr = testUser.split("|");
            String testUserID = testUserarr[0];
            String trnPartn = testUserarr[1];
            String tstPartn = testUserarr[2];

            String[] trnPartnArr = trnPartn.split(",");
            String[] tstPartnArr = tstPartn.split(",");

            for (String trnprtnSubReddt : trnPartnArr) {
                String[] trnsbrdtrtng = trnprtnSubReddt.split("#");
                testUserTrainSubRedditRatingsMap.put(trnsbrdtrtng[0], Double.parseDouble(trnsbrdtrtng[1]));
            }

            for (String tstprtnSubReddt : tstPartnArr) {
                testUserTestSubRedditRatingsList.add((tstprtnSubReddt.split("#"))[0]);
            }

            Double maxsimilarity = -9E9D;
            String closestClusterMember = "";

            for (String clusterMemeberUser : clusterUserSubRedditRatingsMap.keySet()) {
                Map<String, Double> subrdtngmp = clusterUserSubRedditRatingsMap.get(clusterMemeberUser);
                Double similarity = 0.0;
                for (String clustersubReddit : subrdtngmp.keySet()) {
                    for (String tstUserSubReddit : testUserTrainSubRedditRatingsMap.keySet()) {
                        if (clustersubReddit.equals(tstUserSubReddit)) {
                            similarity += subrdtngmp.get(clustersubReddit) * testUserTrainSubRedditRatingsMap.get(tstUserSubReddit) * Double.parseDouble(subRedditWeightMap.get(clustersubReddit));
                        }
                    }
                }
                if (similarity > maxsimilarity) {
                    maxsimilarity = similarity;
                    closestClusterMember = clusterMemeberUser;
                }
            }

            String recommendedSubReddits = "";
            int nofMismatches = 0;

            if (!closestClusterMember.isEmpty()) {

                List<String> closestClusterMemeberSubReddits = new ArrayList<String>((clusterUserSubRedditRatingsMap.get(closestClusterMember)).keySet());
                String canBerecomendedStrings = StringUtils.join(",", closestClusterMemeberSubReddits);
                String tstUserSubReddits = StringUtils.join(",", testUserTestSubRedditRatingsList); //+","+StringUtils.join(",",new ArrayList<String>(testUserTrainSubRedditRatingsMap.keySet()));

                context.write(new Text(testUserID + "#" + tstUserSubReddits), new Text(canBerecomendedStrings));

                closestClusterMemeberSubReddits.clear();
                testUserTestSubRedditRatingsList.clear();
//                for(String tstUsrtstSuReddt : testUserTestSubRedditRatingsList) {
//                    for (String closestClusterMemeberSubReddit : closestClusterMemeberSubReddits) {
//                        if(closestClusterMemeberSubReddit
//                    }
//                }
            }


//            clusterUserSubRedditMap.clear();
            clusterUserSubRedditRatingsMap.clear();
            testUserTrainSubRedditRatingsMap.clear();
            testUserTrainSubRedditRatingsMap.clear();


        } catch (Exception e) {

        }

    }
}
