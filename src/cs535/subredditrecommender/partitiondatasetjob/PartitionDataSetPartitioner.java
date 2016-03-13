package cs535.subredditrecommender.partitiondatasetjob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionDataSetPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numofReducers) {

        if (key.toString().equals("TRAIN")) {
            return 0;
        } else if(key.toString().equals("TEST_1")) {
            return 1;
        } else if(key.toString().equals("TEST_2")) {
            return 2;
        } else if(key.toString().equals("TEST_3")) {
            return 3;
        } else if(key.toString().equals("TEST_4")) {
            return 4;
        } else if(key.toString().equals("TEST_5")) {
            return 5;
        } else if(key.toString().equals("TEST_6")) {
            return 6;
        } else if(key.toString().equals("TEST_7")) {
            return 7;
        } else if(key.toString().equals("TEST_8")) {
            return 8;
        } else if(key.toString().equals("TEST_9")) {
            return 9;
        } else {
            return 10;
        }
    }
}
