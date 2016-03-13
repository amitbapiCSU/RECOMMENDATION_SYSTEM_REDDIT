package cs535.subredditrecommender.dataparsingjob;

import cs535.subredditrecommender.utilities.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import static cs535.subredditrecommender.templates.AllTemplates.mainParsed;

public class DataParsingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Long> subredditCounts = new HashMap<>();
        HashMap<String, Long> subredditScores = new HashMap<>();

        for (Text value : values) {
            CSVParser csvParser = new CSVParser(value.toString(), mainParsed);

            try {
                if (subredditCounts.containsKey(csvParser.get("subreddit_id"))) {
                    subredditCounts.put(csvParser.get("subreddit_id"), subredditCounts.get(csvParser.get("subreddit_id")) + Long.parseLong(csvParser.get("count")));
                } else {
                    subredditCounts.put(csvParser.get("subreddit_id"), Long.parseLong(csvParser.get("count")));
                }

                if (subredditScores.containsKey(csvParser.get("subreddit_id"))) {
                    subredditScores.put(csvParser.get("subreddit_id"), subredditScores.get(csvParser.get("subreddit_id")) + Long.parseLong(csvParser.get("score")));
                } else {
                    subredditScores.put(csvParser.get("subreddit_id"), Long.parseLong(csvParser.get("score")));
                }
            } catch (NumberFormatException e) {

            }
        }

        for (String subName : subredditCounts.keySet()) {
            context.write(key, new Text(subName + "," + subredditCounts.get(subName) + "," + subredditScores.get(subName)));
        }
    }
}
