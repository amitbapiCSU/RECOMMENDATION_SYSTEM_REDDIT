package cs535.subredditrecommender.generic;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueNameReducer extends Reducer<Text, Text, Text, Text> {

}
