import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


// 因为我们使用<classname, word>作为键, 内置的MapWritable只实现了Writable接口不能作为键,
// 所以必须自定义PairWritable实现WritableComparable接口
public class PairWritable implements WritableComparable<PairWritable> {
    private Text classname;
    private Text word;

    public PairWritable() {
        classname = new Text();
        word = new Text();
    }

    public PairWritable(Text classname, Text word){
        this.classname = classname;
        this.word = word;
    }
    public PairWritable(String classname, String word){
        this.classname = new Text(classname);
        this.word = new Text(word);
    }

    public int compareTo(PairWritable o) {
       int cmp = classname.compareTo(o.classname);
       return cmp != 0 ? cmp : word.compareTo(o.word);
    }

    public void write(DataOutput dataOutput) throws IOException {
        classname.write(dataOutput);
        word.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        classname.readFields(dataInput);
        word.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return classname.hashCode() * 163 + word.hashCode();
    }

    public Text getClassname() {
        return classname;
    }

    public void setClassname(Text classname) {
        this.classname = classname;
    }

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PairWritable){
            PairWritable pairWritable = (PairWritable) obj;
            return this.classname.equals(pairWritable.getClassname())
                    && this.word.equals(pairWritable.getWord());
        }

        return false;
    }

    @Override
    public String toString() {
        return this.classname + "\t" + this.word;
    }
}
