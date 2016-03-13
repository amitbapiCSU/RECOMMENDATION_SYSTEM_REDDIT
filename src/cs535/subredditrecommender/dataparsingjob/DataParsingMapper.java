package cs535.subredditrecommender.dataparsingjob;

import cs535.subredditrecommender.utilities.CSVParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

import static cs535.subredditrecommender.templates.AllTemplates.main;

public class DataParsingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser csvParser = new CSVParser(value.toString(), main);

        try {
            Long upvotes = Long.parseLong(csvParser.get("score").trim()) + Long.parseLong(csvParser.get("downs").trim());
            context.write(new Text(csvParser.get("author")), new Text(csvParser.get("subreddit") + ",1," + upvotes.toString()));
        } catch (NumberFormatException e) {

        }
    }
}
