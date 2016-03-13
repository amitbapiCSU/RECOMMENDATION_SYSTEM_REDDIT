package cs535.subredditrecommender.kmeansclusteringjob;

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
import java.text.DecimalFormat;
import java.util.HashMap;

public class KMeansClusteringMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String, HashMap<String, Double>> oldClusters = new HashMap<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem dfs = FileSystem.get(conf);

        Integer totalReduce = Integer.parseInt(conf.get("totalReducers"));

        if (conf.get("initialClusters") == null) {
            throw new RuntimeException("no cluster file!");
        }

        String delim;
        String prefix;
        if (conf.get("initialClusters").contains("canopy")) {
            delim = "\\t";
            prefix = "part";
        } else {
            delim = "\\$";
            prefix = "newCenters";
        }

        DecimalFormat decimalFormat = new DecimalFormat("00000");
        for (int i = 0; i < totalReduce; i++) {
            try {
                read(dfs, conf.get("initialClusters") + "/" + prefix + "-r-" + decimalFormat.format(i), delim);
            } catch (IOException e) {

            }
        }
    }

    private void read(FileSystem dfs, String path, String delim) throws IOException {
        Path src = new Path(path);
        FSDataInputStream fs = dfs.open(src);
        BufferedReader myReader = new BufferedReader(new InputStreamReader(fs));

        String cur = myReader.readLine();
        while (cur != null) {

            String[] entries;


            entries = cur.split(delim);
            String user = entries[0].trim();

            if (!oldClusters.containsKey(user)) {
                oldClusters.put(user, new HashMap<>());
            }

            String[] ratings = entries[1].trim().split(",");

            for (String rating : ratings) {
                String[] subAndRate = rating.split("#");
                oldClusters.get(user).put(subAndRate[0].trim(), Double.parseDouble(subAndRate[1].trim()));
            }
            cur = myReader.readLine();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cur = value.toString();
        String[] entries = cur.split("\\t");
        String user = entries[0].trim();
        String[] ratings = entries[1].trim().split(",");

        String minKey = "";
        Double maxSimilarity = -9E99d;

        for (String cluster : oldClusters.keySet()) {
            if (!minKey.isEmpty()) {
                Double similarity = 0.0d;
                for (String rating : ratings) {
                    String[] subAndRate = rating.split("#");
                    if (oldClusters.get(cluster).containsKey(subAndRate[0].trim())) {
                        double addDistance = oldClusters.get(cluster).get(subAndRate[0].trim()) * Double.parseDouble(subAndRate[1].trim());
                        similarity += addDistance;
                    }
                }

                if (similarity > maxSimilarity) {
                    minKey = cluster;
                    maxSimilarity = similarity;
                }
            } else {
                minKey = cluster;
            }
        }

        context.write(new Text(minKey), value);
    }
}
