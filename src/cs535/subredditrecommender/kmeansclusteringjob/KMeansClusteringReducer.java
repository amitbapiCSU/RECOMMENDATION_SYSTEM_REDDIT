package cs535.subredditrecommender.kmeansclusteringjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

public class KMeansClusteringReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> mos;
    private HashMap<String, List<Double>> clusterPointSubRedditRatingsMap = new HashMap<String, List<Double>>();
    private boolean writeNewCenters = true;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
        Configuration conf = context.getConfiguration();

        if (Integer.parseInt(conf.get("writeNewCenters")) == 0) {
            writeNewCenters = false;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    private Double getAveragedRatingsforCertainSubReddit(List<Double> ratingsList) {

        Double averagedVal;
        Double ratingSum = 0.0D;

        for (Double rtng : ratingsList) {
            ratingSum += rtng;
        }

        averagedVal = ratingSum / ratingsList.size();

        return averagedVal;

    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        String rndmlySelectedClusterCenterUserID = "";

        for (Text value : values) {
            String cur = value.toString();
            String[] entries = cur.split("\\t");
            String user = entries[0].trim();
            String[] ratings = entries[1].trim().split(",");
            rndmlySelectedClusterCenterUserID = user;

            sb.append(entries[0] + "$" + entries[1]).append("|");

            for (String rating : ratings) {
                if (rating.contains("#")) {
                    String[] subAndRate = rating.split("#");
                    if (clusterPointSubRedditRatingsMap.containsKey(subAndRate[0])) {

                        List<Double> sbrdtratings = clusterPointSubRedditRatingsMap.get(subAndRate[0]);
                        sbrdtratings.add(Double.parseDouble(subAndRate[1]));
                        clusterPointSubRedditRatingsMap.put(subAndRate[0], sbrdtratings);

                    } else {

                        List<Double> sbrdtratings = new ArrayList<Double>();
                        sbrdtratings.add(Double.parseDouble(subAndRate[1]));
                        clusterPointSubRedditRatingsMap.put(subAndRate[0], sbrdtratings);

                    }
                }
            }
        }

        StringBuilder keysb = new StringBuilder();
        keysb.append(rndmlySelectedClusterCenterUserID).append("$");

        HashMap<String, Double> sortedAverageMap = new HashMap<>();

        for (String subReddit : clusterPointSubRedditRatingsMap.keySet()) {
            Double localAverage = getAveragedRatingsforCertainSubReddit(clusterPointSubRedditRatingsMap.get(subReddit));
            sortedAverageMap.put(subReddit, localAverage);
        }

        List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(sortedAverageMap.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        for (int i = 0; i < (list.size() > 100 ? 100 : list.size()); i++) {
            keysb.append(list.get(i).getKey()).append("#").append(list.get(i).getValue()).append(",");
            if (i == list.size() - 1) {
                break;
            }
        }

        clusterPointSubRedditRatingsMap.clear();
        sortedAverageMap.clear();
        list.clear();

        context.write(new Text(keysb.toString() + "|" + sb.toString()), new Text(""));

        if (writeNewCenters) {
            mos.write("newCenters", new Text(keysb.toString()), new Text(""));
        }
    }
}