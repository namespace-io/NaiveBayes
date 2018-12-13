
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class CWRecordReader extends RecordReader<PairWritable, IntWritable> {
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private Text classname = null;
    private String cn = null;
    private Text word = null;
    private static IntWritable one = new IntWritable(1);

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        cn = split.getPath().getParent().getName();
        start = split.getStart();
        end  = start + split.getLength();
        final Path file =  split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn =  fs.open(split.getPath());
        boolean skipFirstLine = false;

        if(start != 0){
            skipFirstLine = true;
            --start;
            fileIn.seek(start);
        }
        in = new LineReader(fileIn, job);

        if(skipFirstLine){
            start += in.readLine(new Text(), 0,
                    (int)Math.min((long)Integer.MAX_VALUE, end - start));
        }

        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(word == null){
            word = new Text();
        }

        if(classname == null){
            classname = new Text(cn);
        }

        int newSize = 0;

        while(pos < end){
            newSize = in.readLine(word, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                    maxLineLength));

            if(newSize == 0){
                break;
            }

            pos += newSize;

            if(newSize < maxLineLength){
                break;
            }
        }

        if(newSize == 0){
            word = null;
            classname = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public PairWritable getCurrentKey() throws IOException, InterruptedException {
        System.out.println(classname.toString() + "-" +word.toString());
        return new PairWritable(classname.toString(), word.toString());
    }

    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return one;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
