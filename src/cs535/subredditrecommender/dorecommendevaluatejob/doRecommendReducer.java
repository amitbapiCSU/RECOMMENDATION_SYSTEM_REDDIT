package cs535.subredditrecommender.dorecommendevaluatejob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class doRecommendReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> mos;
//    private String iterationCount = "";
//
//    @Override
//    public void setup(Context context) {
//        mos = new MultipleOutputs<>(context);
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        mos.close();
//        Configuration conf = context.getConfiguration();
//        iterationCount = conf.get("ITERATION_COUNT");
//
//    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {
            Map<Double, String> WeightandSubredditMap = new TreeMap<>();
            Integer numofRecommendedSubreddits = 40;
            String tstUserSubReddits = "";
            for (Text val : values) {

                String tstUserSubredditsrecommendedSubredditswithAccumWeightstr = val.toString();
                String[] arr = tstUserSubredditsrecommendedSubredditswithAccumWeightstr.split("\\$");
                tstUserSubReddits += arr[0];
                String recommendedSubredditswithAccumWeightstr = arr[1];
                String[] recommendedSubredditswithAccumWeightarr = recommendedSubredditswithAccumWeightstr.split("\\,");

                for (String rcmndSubRdtAccumWeight : recommendedSubredditswithAccumWeightarr) {
                    String[] sbrdtWeightarr = rcmndSubRdtAccumWeight.split("\\#");
                    WeightandSubredditMap.put(-Double.parseDouble(sbrdtWeightarr[1]), sbrdtWeightarr[0]);
                }
            }

            Set set = WeightandSubredditMap.entrySet();
            Iterator i = set.iterator();
            Integer idx = 0;
            StringBuilder sb = new StringBuilder();
            while (i.hasNext()) {
                if (idx < numofRecommendedSubreddits) {
                    Map.Entry entry = (Map.Entry) i.next();
                    sb.append(entry.getValue().toString()).append(",");
                } else {
                    break;
                }
                idx++;
            }

//            mos.write("recommend_"+iterationCount, new Text(key.toString() + "$" + tstUserSubReddits), new Text(sb.toString()));
            context.write(new Text(key.toString() + "$" + tstUserSubReddits), new Text(sb.toString()));

            WeightandSubredditMap.clear();


        } catch (Exception e) {

        }

    }
}
