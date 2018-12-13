import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;


public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        PrioriProbability priori = new PrioriProbability();
//        ToolRunner.run(conf, priori, args);
//
//        ConditionalProbability cond = new ConditionalProbability();
//        ToolRunner.run(conf, cond, args);

        Prediction pred = new Prediction();
        ToolRunner.run(conf, pred, args);


        // read dir
        HashMap<String, String> docTrueClass = new HashMap<String, String>();
        FileSystem fs = FileSystem.get(conf);
        Path test_input_path = new Path(Utils.TEST_INPUT_PATH);
        FileStatus[] status = fs.listStatus(test_input_path);
        ArrayList<String> C = new ArrayList<String>();

        for (FileStatus stat: status){
            if(stat.isDirectory()){
                Path dir = stat.getPath();
                String dirName = dir.getName();
                C.add(dirName);
                FileStatus[] files = fs.listStatus(dir);
                for(FileStatus fileStatus : files){
                    docTrueClass.put(fileStatus.getPath().getName(), dirName);
                }
            }
        }


//        for(String c : docTrueClass.keySet()){
//            System.out.println( c + "真实属于 " + docTrueClass.get(c));
//        }

        double P;
        double R;
        double F1;

        double totalP = 0, totalR = 0, totalF1 = 0, totalC = C.size(), totalTP = 0, totalFN = 0, totalFP = 0;
        int TP , TN, FP, FN ;
        HashMap<String, String> predDoc = new HashMap<String, String>();

        Path result = new Path(Utils.TEST_RES_OUTPUT_PATH  + Utils.RESULT);
        FSDataInputStream fsr = fs.open(result);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        String line;
        String[] docClass;

        while ((line = bufferedReader.readLine()) != null){
            docClass = line.split("\\s+");
            predDoc.put(docClass[0], docClass[1]);
        }

//        for(String c : predDoc.keySet()){
//            System.out.println("预测 " +c + " 属于 " + predDoc.get(c));
//        }

        int K = 0;
        for(String c : C){
            TP = 0;
            TN = 0;
            FP = 0;
            FN = 0;
            for(String doc : predDoc.keySet()){
                String classname = docTrueClass.get(doc);
                String predClass = predDoc.get(doc);
                if (classname.equals(c) && predClass.equals(c)) {
                    K++;
                    TP += 1;
                } else if(classname.equals(c) && !predClass.equals(c)){
                    FN += 1;
                } else if(!classname.equals(c) && predClass.equals(c)){
                    FP += 1;
                } else {
                    TN += 1;
                }
            }
            P = (double) TP / (TP + FP);
            R = (double) TP / (TP + FN);
            F1 = 2*P*R/(P+R);
            System.out.println("类别" + c + " 的P值为" + P + " , R值为" + R + ", F1值为" + F1);
            totalTP += TP;
            totalFP += FP;
            totalFN += FN;
            totalP += P;
            totalR += R;
            totalF1 += F1;
        }


        System.out.println("总共预测成功 " + K + " 个文档");
        double  MacroP = totalP / totalC,
                MacroR = totalR / totalC,
                MacroF1 = totalF1 / totalC;
        double  MicroP =  totalTP / (totalTP + totalFP),
                MicroR = totalTP /(totalTP + totalFN),
                MicroF1 = 2 * MicroP * MicroR / (MicroP + MicroR);

        System.out.println("Macro: " + "Precision = " + MacroP +", Recall = " + MacroR+", F1 = " + MacroF1);
        System.out.println("Micro: " + "Precision = " + MicroP +", Recall = " + MicroR+", F1 = " + MicroF1);

    }
}
