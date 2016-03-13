package cs535.subredditrecommender.calculatecanopycenterjob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CalculateCanopyCenterReducer extends Reducer<Text, Text, Text, Text> {

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
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {

            for (Text val : values) {
                String SubRedditIDRtngsValue = val.toString();
                ArrayList<String> SubRedditIDRtngsData = new ArrayList<String>();

                String[] subRedditrtngArr = SubRedditIDRtngsValue.split(",");
                for (String subrdtrtng : subRedditrtngArr) {
                    SubRedditIDRtngsData.add((subrdtrtng.split("#"))[1]);
                }
                String UserKey = key.toString();

                boolean isClose = false;

                for (ArrayList<String> center : canopyCenters) {
                    if (measureDistance(center, SubRedditIDRtngsData) <= T1) {
                        //Center is too close
                        isClose = true;
                        break;
                    }
                }

                if (!isClose) {
                    canopyCenters.add(SubRedditIDRtngsData);
                    context.write(new Text(UserKey), new Text(SubRedditIDRtngsValue));
                }
            }


        } catch (Exception e) {

        }

    }

}
