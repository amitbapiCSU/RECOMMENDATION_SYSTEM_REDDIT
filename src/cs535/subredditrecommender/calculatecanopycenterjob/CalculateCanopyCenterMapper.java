package cs535.subredditrecommender.calculatecanopycenterjob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class CalculateCanopyCenterMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ArrayList<ArrayList<String>> canopyCenters;
    private Double T1 = 6.0D;

    private double measureDistance(ArrayList<String> origin, ArrayList<String> destination) {
        double deltaSum = 0.0;

        for (int i = 0; i < origin.size(); i++) {
            if (destination.size() > i) {
                deltaSum = deltaSum + Math.pow(Math.abs(Double.valueOf(origin.get(i)) - Double.valueOf(destination.get(i))), 2);
            }
        }
        return Math.sqrt(deltaSum);
    }

    @Override
    public void setup(Context context) {
        this.canopyCenters = new ArrayList<ArrayList<String>>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            String input_line = value.toString().trim();
            String[] linearr = input_line.split("\t");

            String UserKey = linearr[0];
            String SubRedditIDVal = linearr[1];

            ArrayList<String> SubRedditData = new ArrayList<String>();//new ArrayList<String>(Arrays.asList(SubRedditIDVal.split(",")));

            String[] subRedditrtngArr = SubRedditIDVal.split(",");
            for (String subrdtrtng : subRedditrtngArr) {
                SubRedditData.add((subrdtrtng.split("#"))[1]);
            }

            boolean isClose = false;

            for (ArrayList<String> center : canopyCenters) {
                if (measureDistance(center, SubRedditData) <= T1) {
                    //Center is too close
                    isClose = true;
                    break;
                }
            }

            if (!isClose) {
                canopyCenters.add(SubRedditData);
                context.write(new Text(UserKey), new Text(SubRedditIDVal));
            }

        } catch (Exception e) {

        }

    }

}
