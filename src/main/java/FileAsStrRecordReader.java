import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;


public class FileAsStrRecordReader extends RecordReader<Text, Text> {
    private FileSplit split;
    private boolean processed = false;
    private Text docID ;
    private Text content;
    private Configuration conf;
    private static final Log LOG = LogFactory.getLog(FileAsStrRecordReader.class);

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(docID == null){
            docID = new Text();
        }

        if(content == null){
            content = new Text();
        }

        if(!processed){
            byte[] bytes = new byte[(int) split.getLength()];
            Path doc = split.getPath();
            FileSystem fs = split.getPath().getFileSystem(conf);

            docID.set(doc.getName());

            FSDataInputStream in = fs.open(doc);
            try {
                IOUtils.readFully(in, bytes, 0, bytes.length);
                content.set(bytes, 0, bytes.length);
            } catch (IOException e){
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(in);
                fs.close();
            }

            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return docID;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return content;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f: 0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}