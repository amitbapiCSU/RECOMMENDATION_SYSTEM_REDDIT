package cs535.subredditrecommender.aggregateclustersubredditsjob;

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

public class AggregateClusterSubredditsMapper extends Mapper<LongWritable, Text, Text, Text> {

    HashMap<String, Double> subWeights = new HashMap<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem dfs = FileSystem.get(conf);

        if (conf.get("subredditWeights") == null) {
            throw new RuntimeException("no cluster file!");
        }

        Path src = new Path(conf.get("subredditWeights"));
        FSDataInputStream fs = dfs.open(src);
        BufferedReader myReader = new BufferedReader(new InputStreamReader(fs));

        String cur = myReader.readLine();
        while (cur != null) {
            String[] entries = cur.split("\\t");

            String subreddit = entries[0].trim();
            String weight = entries[1].trim();

            subWeights.put(subreddit, Double.parseDouble(weight));


            cur = myReader.readLine();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> clusterRatings = new HashMap<>();

        String cur = value.toString();
        String[] entries = cur.split("\\|");
        String user = entries[0].split("\\$")[0];


        for (int i = 1; i < entries.length; i++) {
            if (entries[i] != null && !entries[i].isEmpty()) {

                String[] ratingMapping = entries[i].split("\\$");

                if (ratingMapping.length == 2) {
                    String[] subRatings = ratingMapping[1].split("\\,");
                    for (String subRating : subRatings) {
                        if (!subRating.isEmpty()) {
                            String[] sub = subRating.split("\\#");
                            if (subWeights.containsKey(sub[0].trim())) {
                                if (clusterRatings.containsKey(sub[0].trim())) {
                                    clusterRatings.put(sub[0].trim(), clusterRatings.get(sub[0].trim()) + (Double.parseDouble(sub[1].trim()) / subWeights.get(sub[0].trim())));
                                } else {
                                    clusterRatings.put(sub[0].trim(), Double.parseDouble(sub[1].trim()) / subWeights.get(sub[0].trim()));
                                }
                            }
                        }
                    }
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Double> entry : clusterRatings.entrySet()) {
            sb.append(entry.getKey()).append("#").append(entry.getValue()).append(",");
        }

        context.write(new Text(user), new Text(sb.toString()));
    }
}
