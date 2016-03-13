package cs535.subredditrecommender.driver;

import cs535.subredditrecommender.aggregateclustersubredditsjob.AggregateClusterSubredditsMapper;
import cs535.subredditrecommender.aggregateclustersubredditsjob.AggregateClusterSubredditsReducer;
import cs535.subredditrecommender.calculatecanopycenterjob.CalculateCanopyCenterMapper;
import cs535.subredditrecommender.calculatecanopycenterjob.CalculateCanopyCenterReducer;
import cs535.subredditrecommender.calculateratingjob.CalculateRatingMapper;
import cs535.subredditrecommender.calculateratingjob.CalculateRatingReducer;
import cs535.subredditrecommender.calculatesubredditweightjob.CalculateSubredditWeightMapper;
import cs535.subredditrecommender.calculatesubredditweightjob.CalculateSubredditWeightReducer;
import cs535.subredditrecommender.dataparsingjob.DataParsingMapper;
import cs535.subredditrecommender.dataparsingjob.DataParsingReducer;
import cs535.subredditrecommender.dorecommendevaluatejob.doRecommendMapper;
import cs535.subredditrecommender.dorecommendevaluatejob.doRecommendReducer;
import cs535.subredditrecommender.initializeratematrixjob.InitializeRateMatrixMapper;
import cs535.subredditrecommender.initializeratematrixjob.InitializeRateMatrixReducer;
import cs535.subredditrecommender.kmeansclusteringjob.KMeansClusteringMapper;
import cs535.subredditrecommender.kmeansclusteringjob.KMeansClusteringReducer;
import cs535.subredditrecommender.partitiondatasetjob.PartitionDataSetMapper;
import cs535.subredditrecommender.partitiondatasetjob.PartitionDataSetPartitioner;
import cs535.subredditrecommender.partitiondatasetjob.PartitionDataSetReducer;
import cs535.subredditrecommender.utilities.RatingsInformationClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;

public class Main extends Configured implements Tool {
    private static final Integer TOTAL_ITERATIONS = 6;
    private static String MAIN_DATASET = "/CS535ProjectDemo/";   // CS535ProjectData
    private static String OUTPUT_FILE = "/CS535Reddit";
    private static Integer REDUCE_TASKS = 40;
    private String NO_OF_ELEMETS = "";

    public static void main(String[] args) throws Exception {
        //MAIN_DATASET = args[1];
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isDone;

        isDone = parseData();
        if (!isDone) return 1;

        isDone = initializeRateMatrix();
        if (!isDone) return 1;

        isDone = calculateRating();
        if (!isDone) return 1;

        isDone = partitionDataSet();
        if (!isDone) return 1;

        isDone = calculateSubredditWeight();
        if (!isDone) return 1;

        isDone = calculateCanopyCenter();
        if (!isDone) return 1;

        for (Integer i = 0; i < TOTAL_ITERATIONS; i++) {
            isDone = kMeansClustering(i);    //TODO: Loop
            if (!isDone) return 1;
        }

        isDone = aggregateClusterSubreddits();
        if (!isDone) return 1;


        for (int i = 1; i <= 10; i++) {
            isDone = doRecommendandEvaluate(i);
            if (!isDone) return 1;
        }

        return 0;
    }

    private boolean parseData() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job dataParsingJob = Job.getInstance(conf, "dataParsingJob");
        dataParsingJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(dataParsingJob, new Path(MAIN_DATASET));

        FileOutputFormat.setOutputPath(dataParsingJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_parsedData"));

        MultipleOutputs.addNamedOutput(dataParsingJob, "usermap", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(dataParsingJob, "subredditmap", TextOutputFormat.class, Text.class, Text.class);

        LazyOutputFormat.setOutputFormatClass(dataParsingJob, TextOutputFormat.class);

        dataParsingJob.setMapOutputKeyClass(Text.class);

        dataParsingJob.setReducerClass(DataParsingReducer.class);

        dataParsingJob.setMapperClass(DataParsingMapper.class);

        dataParsingJob.setCombinerClass(DataParsingReducer.class);

        dataParsingJob.setOutputKeyClass(Text.class);
        dataParsingJob.setOutputValueClass(Text.class);

        return dataParsingJob.waitForCompletion(true);
    }

    private boolean initializeRateMatrix() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job initializeRateMatrixJob = Job.getInstance(conf, "initializeRateMatrixJob");
        initializeRateMatrixJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(initializeRateMatrixJob, new Path(OUTPUT_FILE + "_parsedData/part-r-00000"));

        FileOutputFormat.setOutputPath(initializeRateMatrixJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_rateMatrixVariables"));

        LazyOutputFormat.setOutputFormatClass(initializeRateMatrixJob, TextOutputFormat.class);

        initializeRateMatrixJob.setMapOutputKeyClass(Text.class);

        initializeRateMatrixJob.setReducerClass(InitializeRateMatrixReducer.class);

        initializeRateMatrixJob.setMapperClass(InitializeRateMatrixMapper.class);

        initializeRateMatrixJob.setOutputKeyClass(Text.class);
        initializeRateMatrixJob.setOutputValueClass(Text.class);

        return initializeRateMatrixJob.waitForCompletion(true);
    }

    private boolean calculateRating() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job ratingCalculationJob = Job.getInstance(conf, "ratingCalculationJob");
        ratingCalculationJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(ratingCalculationJob, new Path(OUTPUT_FILE + "_rateMatrixVariables"));

        FileOutputFormat.setOutputPath(ratingCalculationJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_rateMatrix"));

        LazyOutputFormat.setOutputFormatClass(ratingCalculationJob, TextOutputFormat.class);

        ratingCalculationJob.setMapOutputKeyClass(Text.class);

        ratingCalculationJob.setMapOutputValueClass(RatingsInformationClass.class);

        ratingCalculationJob.setReducerClass(CalculateRatingReducer.class);

        ratingCalculationJob.setMapperClass(CalculateRatingMapper.class);

        ratingCalculationJob.setOutputKeyClass(Text.class);
        ratingCalculationJob.setOutputValueClass(Text.class);

        boolean isComplete = ratingCalculationJob.waitForCompletion(true);

        Counters counters = ratingCalculationJob.getCounters();
        long outputBytes = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_GROUPS").getValue();
        NO_OF_ELEMETS = Double.toString(outputBytes);

        return isComplete;
    }

    private boolean partitionDataSet() throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        conf.set("NO_of_Users", NO_OF_ELEMETS);
        Job partitionDataSetJob = Job.getInstance(conf, "partitionDataSetJob");
        partitionDataSetJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(partitionDataSetJob, new Path(OUTPUT_FILE + "_rateMatrix"));

        FileOutputFormat.setOutputPath(partitionDataSetJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_PartitionedDataSet"));

        LazyOutputFormat.setOutputFormatClass(partitionDataSetJob, TextOutputFormat.class);

        partitionDataSetJob.setMapOutputKeyClass(Text.class);

        partitionDataSetJob.setMapOutputValueClass(Text.class);

        partitionDataSetJob.setReducerClass(PartitionDataSetReducer.class);

        partitionDataSetJob.setPartitionerClass(PartitionDataSetPartitioner.class);

        partitionDataSetJob.setMapperClass(PartitionDataSetMapper.class);

        partitionDataSetJob.setOutputKeyClass(Text.class);
        partitionDataSetJob.setOutputValueClass(Text.class);

        partitionDataSetJob.setNumReduceTasks(11);

        return partitionDataSetJob.waitForCompletion(true);
    }

    private boolean calculateSubredditWeight() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job calculateSubredditWeightJob = Job.getInstance(conf, "calculateSubredditWeightJob");
        calculateSubredditWeightJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(calculateSubredditWeightJob, new Path(OUTPUT_FILE + "_rateMatrix"));

        FileOutputFormat.setOutputPath(calculateSubredditWeightJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_subredditWeights"));

        LazyOutputFormat.setOutputFormatClass(calculateSubredditWeightJob, TextOutputFormat.class);

        calculateSubredditWeightJob.setMapOutputKeyClass(Text.class);

        calculateSubredditWeightJob.setMapOutputValueClass(Text.class);

        calculateSubredditWeightJob.setReducerClass(CalculateSubredditWeightReducer.class);

        calculateSubredditWeightJob.setMapperClass(CalculateSubredditWeightMapper.class);

        calculateSubredditWeightJob.setOutputKeyClass(Text.class);
        calculateSubredditWeightJob.setOutputValueClass(Text.class);

        return calculateSubredditWeightJob.waitForCompletion(true);
    }

    private boolean calculateCanopyCenter() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job calculateCanopyCenterJob = Job.getInstance(conf, "calculateCanopyCenterJob");
        calculateCanopyCenterJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(calculateCanopyCenterJob, new Path(OUTPUT_FILE + "_PartitionedDataSet/part-r-00000"));

        FileOutputFormat.setOutputPath(calculateCanopyCenterJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_canopyCenters"));

        LazyOutputFormat.setOutputFormatClass(calculateCanopyCenterJob, TextOutputFormat.class);

        calculateCanopyCenterJob.setMapOutputKeyClass(Text.class);

        calculateCanopyCenterJob.setMapOutputValueClass(Text.class);

        calculateCanopyCenterJob.setReducerClass(CalculateCanopyCenterReducer.class);

        calculateCanopyCenterJob.setMapperClass(CalculateCanopyCenterMapper.class);

        calculateCanopyCenterJob.setNumReduceTasks(REDUCE_TASKS);

        calculateCanopyCenterJob.setOutputKeyClass(Text.class);
        calculateCanopyCenterJob.setOutputValueClass(Text.class);

        return calculateCanopyCenterJob.waitForCompletion(true);
    }

    private boolean kMeansClustering(Integer iteration) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        long milliSeconds = 1000 * 60 * 60;
        conf.setLong("mapred.task.timeout", milliSeconds);

        conf.set("totalReducers", REDUCE_TASKS.toString());
        if (iteration == 0) {
            conf.set("initialClusters", OUTPUT_FILE + "_canopyCenters");
        } else {
            conf.set("initialClusters", OUTPUT_FILE + "_newClusters_" + (iteration - 1));
        }

        conf.set("writeNewCenters", "1");

        if (iteration == TOTAL_ITERATIONS - 1) {
            conf.set("writeNewCenters", "0");
        }

        Job kMeansClusteringJob = Job.getInstance(conf, "kMeansClusteringJob");
        kMeansClusteringJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(kMeansClusteringJob, new Path(OUTPUT_FILE + "_PartitionedDataSet/part-r-00000"));

        FileOutputFormat.setOutputPath(kMeansClusteringJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_newClusters_" + iteration.toString()));

        MultipleOutputs.addNamedOutput(kMeansClusteringJob, "newCenters", TextOutputFormat.class, Text.class, Text.class);

        LazyOutputFormat.setOutputFormatClass(kMeansClusteringJob, TextOutputFormat.class);

        kMeansClusteringJob.setMapOutputKeyClass(Text.class);

        kMeansClusteringJob.setMapOutputValueClass(Text.class);

        kMeansClusteringJob.setReducerClass(KMeansClusteringReducer.class);

        kMeansClusteringJob.setMapperClass(KMeansClusteringMapper.class);

        kMeansClusteringJob.setNumReduceTasks(REDUCE_TASKS);

        kMeansClusteringJob.setOutputKeyClass(Text.class);
        kMeansClusteringJob.setOutputValueClass(Text.class);

        return kMeansClusteringJob.waitForCompletion(true);
    }

    private boolean aggregateClusterSubreddits() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("subredditWeights", OUTPUT_FILE + "_subredditWeights/part-r-00000");
        Job aggregateClusterSubredditsJob = Job.getInstance(conf, "aggregateClusterSubredditsJob");

        aggregateClusterSubredditsJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(aggregateClusterSubredditsJob, new Path(OUTPUT_FILE + "_newClusters_" + (TOTAL_ITERATIONS - 1)));

        FileOutputFormat.setOutputPath(aggregateClusterSubredditsJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_clusteredSubreddits"));

        LazyOutputFormat.setOutputFormatClass(aggregateClusterSubredditsJob, TextOutputFormat.class);

        aggregateClusterSubredditsJob.setMapOutputKeyClass(Text.class);

        aggregateClusterSubredditsJob.setMapOutputValueClass(Text.class);

        aggregateClusterSubredditsJob.setReducerClass(AggregateClusterSubredditsReducer.class);

        aggregateClusterSubredditsJob.setMapperClass(AggregateClusterSubredditsMapper.class);

        aggregateClusterSubredditsJob.setOutputKeyClass(Text.class);
        aggregateClusterSubredditsJob.setOutputValueClass(Text.class);

        return aggregateClusterSubredditsJob.waitForCompletion(true);
    }

    private boolean doRecommendandEvaluate(int fileIndex) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        DecimalFormat decimalFormat = new DecimalFormat("00000");

        conf.set("CLUSER_SUBREDDIT_ACCUMULATED_WEIGHT", OUTPUT_FILE + "_clusteredSubreddits/part-r-00000");
//        conf.set("ITERATION_COUNT" , String.valueOf(fileIndex));

        Job doRecommendandEvaluateJob = Job.getInstance(conf, "doRecommendandEvaluateJob");
        doRecommendandEvaluateJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(doRecommendandEvaluateJob, new Path(OUTPUT_FILE + "_PartitionedDataSet/part-r-"+decimalFormat.format(fileIndex)));

        FileOutputFormat.setOutputPath(doRecommendandEvaluateJob, deleteIfExistsAndGetPath(OUTPUT_FILE + "_RECOMMENDATION_"+decimalFormat.format(fileIndex)));

//        MultipleOutputs.addNamedOutput(doRecommendandEvaluateJob, "recommend_" + fileIndex, TextOutputFormat.class, Text.class, Text.class);

        LazyOutputFormat.setOutputFormatClass(doRecommendandEvaluateJob, TextOutputFormat.class);

        doRecommendandEvaluateJob.setMapOutputKeyClass(Text.class);

        doRecommendandEvaluateJob.setMapOutputValueClass(Text.class);

        doRecommendandEvaluateJob.setReducerClass(doRecommendReducer.class);

        doRecommendandEvaluateJob.setMapperClass(doRecommendMapper.class);

        doRecommendandEvaluateJob.setOutputKeyClass(Text.class);
        doRecommendandEvaluateJob.setOutputValueClass(Text.class);

//        doRecommendandEvaluateJob.setNumReduceTasks(REDUCE_TASKS);

        return doRecommendandEvaluateJob.waitForCompletion(true);

    }

    private Path deleteIfExistsAndGetPath(String file) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        Path p = new Path(file);
        if (fs.exists(p)) fs.delete(p, true);
        return p;
    }
}
