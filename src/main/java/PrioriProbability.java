import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PrioriProbability extends Configured implements Tool {
    public static class ClassCountMapper
            extends Mapper<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    public static class ClassSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();

        Path inputPath = new Path(Utils.TRAINING_INPUT_PATH);
        Path outputPath = new Path(Utils.PRIORI_RES_OUTPUT_PATH);

        FileSystem fs = outputPath.getFileSystem(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        Job job = Job.getInstance(conf, "PrioriProbability");
        job.setJarByClass(PrioriProbability.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setMapperClass(ClassCountMapper.class);
        job.setReducerClass(ClassSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem trainSet = inputPath.getFileSystem(conf);
        FileStatus[] fileStatuses = trainSet.listStatus(inputPath);
        for(FileStatus fileStatus : fileStatuses){
            FileInputFormat.addInputPath(job, new Path(fileStatus.getPath().toString()));
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PrioriProbability(), args);
        System.exit(res);
    }
}
