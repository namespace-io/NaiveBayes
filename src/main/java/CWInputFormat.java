import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CWInputFormat extends FileInputFormat<PairWritable, IntWritable> {

    @Override
    public RecordReader<PairWritable, IntWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        CWRecordReader reader = new CWRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
