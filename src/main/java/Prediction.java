import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;


public class Prediction extends Configured implements Tool {
    private static Hashtable<String, Double> Priori= new Hashtable<String, Double>();
    private static Hashtable<Pair<String, String>, Double> Cond = new Hashtable<Pair<String, String>, Double>();
    private static Hashtable<String, Integer> TotalWordInClass = new Hashtable<String, Integer>();
    private static final Log LOG = LogFactory.getLog(Prediction.class);
    private static int allwords;
    public static class PredictionMapper extends Mapper<Text, Text, Text, MapWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();

            FileSystem fs;
            FSDataInputStream fsr;
            String[] arrs;
            String classname;
            String word;
            Pair<String, String> cw;
            BufferedReader bufferedReader;
            String line;

            int total = 0, num, counter;
            //calculate and load prior

            final Path prior = new Path(Utils.PRIORI_RES_OUTPUT_PATH + Utils.RESULT);

            fs = prior.getFileSystem(conf);
            fsr = fs.open(prior);
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));

            while((line = bufferedReader.readLine()) != null){
                arrs = line.split("\\s+");
                num = Integer.parseInt(arrs[1]);
                total += num;
                Priori.put(arrs[0], (double)num);
            }

            for(String cn : Priori.keySet()){
                Priori.put(cn, Priori.get(cn) / total);
            }

            // calculate and load cond
            final Path cond = new Path(Utils.COND_RES_OUTPUT_PATH + Utils.RESULT);
            fs = cond.getFileSystem(conf);
            fsr = fs.open(cond);
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));


            while ((line = bufferedReader.readLine()) != null){
                arrs = line.split("\\s+");
                classname = arrs[0];
                word = arrs[1];
                counter = Integer.parseInt(arrs[2]);

                allwords += counter;
                if (TotalWordInClass.containsKey(classname)){
                    num = TotalWordInClass.get(classname);
                    TotalWordInClass.put(classname, num + counter);
                } else {
                    TotalWordInClass.put(classname, counter);
                }

                cw = new Pair<String, String>(classname, word);
                if(Cond.containsKey(cw)){
                    Cond.put(cw, Cond.get(cw) + counter);
                } else {
                    Cond.put(cw, (double)counter);
                }
            }

            for (Pair<String, String> pair : Cond.keySet()) {
                Cond.put(pair, (1 + Cond.get(pair)) / (TotalWordInClass.get(pair.getKey()) +  Priori.keySet().size() ) );
            }
            super.setup(context);
        }
        @Override
        protected void map(Text docID, Text value, Context context) throws IOException, InterruptedException {
            DoubleWritable p = new DoubleWritable();
            Text classname = new Text();
            String content = value.toString();

            LOG.info("KeySet = " + Priori.keySet().toString());
            for (String cn : Priori.keySet()){
                MapWritable mw = new MapWritable();
                p.set(ProbabilityForClass(content, cn));
                classname.set(cn);
                LOG.info("MAP : 概率 = " + p.toString() + ", classname = " + classname + ", docID = " + docID.toString());
                mw.put(classname, p);
                LOG.info("MAP mw : " + mw.toString());
                context.write(docID, mw);
            }
        }
    }

    public static class PredictionReducer extends Reducer<Text, MapWritable, Text, Text>{
        @Override
        protected void reduce(Text docID, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            double maxP = -999999;
            Text predClass = new Text();
            for (MapWritable mw: values){
                for(Writable classname : mw.keySet()){
                    DoubleWritable p = (DoubleWritable) mw.get(classname);
                    LOG.info("class : " + classname + " 概率= " + p.get());
                    if (p.get() > maxP){
                        maxP = p.get();
                        predClass.set(classname.toString());
                    }
                }
            }

            LOG.info("REDUCE: " + docID.toString() + " 最大概率= " + maxP + " 类名: " + predClass.toString());
            context.write(docID, predClass);
        }
    }

    private static double ProbabilityForClass(String content, String classname){
        String[] words = content.split("\\s+");
        Pair<String, String> cw;
        double p = 0.0;
        for (String word : words) {
            cw = new Pair<String, String>(classname, word);
            if (Cond.containsKey(cw)) {
                p += Math.log10(Cond.get(cw));
            } else {
                p += Math.log10(1.0 / (TotalWordInClass.get(classname) + Priori.keySet().size()));
            }

        }

        p += Math.log10(Priori.get(classname));
        return p;
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        // load testSet
        Path inputPath = new Path(Utils.TEST_INPUT_PATH);
        Path outputPath = new Path(Utils.TEST_RES_OUTPUT_PATH);


        FileSystem fs = outputPath.getFileSystem(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        Job job = Job.getInstance(conf, "Prediction");


        job.setJarByClass(Prediction.class);
        job.setInputFormatClass(FileAsStrInputFormat.class);
        job.setMapperClass(PredictionMapper.class);
        job.setReducerClass(PredictionReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);



        FileSystem trainSet = inputPath.getFileSystem(conf);
        FileStatus[] fileStatuses = trainSet.listStatus(inputPath);
        for(FileStatus fileStatus : fileStatuses){
            FileInputFormat.addInputPath(job, new Path(fileStatus.getPath().toString()));
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Prediction(), args);
        System.exit(res);
    }
}
