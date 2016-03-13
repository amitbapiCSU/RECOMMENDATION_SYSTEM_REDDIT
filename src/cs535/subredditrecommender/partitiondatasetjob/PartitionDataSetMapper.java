package cs535.subredditrecommender.partitiondatasetjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class PartitionDataSetMapper extends Mapper<LongWritable, Text, Text, Text> {


    private Double TrainSetSize = 0.0;
    private Double no_f_users = 0.0;
    private Double partition_coeff = 0.86D;
    private Double DiffModnofPartitions = 0.0D;
    private Integer nofPartitions = 10;

    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration cnf = context.getConfiguration();
        no_f_users = Double.parseDouble(cnf.get("NO_of_Users"));
        TrainSetSize = no_f_users * partition_coeff;
        DiffModnofPartitions = (no_f_users - TrainSetSize)/ nofPartitions;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {


            double random = new Random().nextDouble();
            double sampleIndex = random * no_f_users;

            if (sampleIndex <= TrainSetSize) {

                context.write(new Text("TRAIN"), value);

            } else {

                for (int i = 1; i <= nofPartitions; i++) {

                    Double startPratitionIndex = (TrainSetSize + (i-1)*DiffModnofPartitions);
                    Double endPratitionIndex = (TrainSetSize + (i)*DiffModnofPartitions);

                    if ((sampleIndex > startPratitionIndex) && (sampleIndex <= endPratitionIndex)) {
                        context.write(new Text("TEST_"+String.valueOf(i)), value);
                        break;
                    }

                }
//                if(sampleIndex <= TrainSetSize+DiffMod5 ) {
//
//                    context.write(new Text("TEST_1"), value);
//
//                } else if ((sampleIndex > TrainSetSize+DiffMod5) && (sampleIndex <= TrainSetSize+2*DiffMod5)){
//
//                    context.write(new Text("TEST_2"), value);
//
//                } else if ((sampleIndex > TrainSetSize+2*DiffMod5) && (sampleIndex <= TrainSetSize+3*DiffMod5)) {
//
//                    context.write(new Text("TEST_3"), value);
//
//                } else if ((sampleIndex > TrainSetSize+3*DiffMod5) && (sampleIndex <= TrainSetSize+4*DiffMod5)) {
//
//                    context.write(new Text("TEST_4"), value);
//
//                } else {
//
//                    context.write(new Text("TEST_5"), value);
//
//                }

            }

        } catch (Exception e) {

        }

    }
}
