package com.example;

import com.example.jobs.Step1_ExtractAndTotals;
import com.example.jobs.Step2_ComputeMI;
import com.example.jobs.Step3_ComputeDenom;
import com.example.jobs.Step4_IntersectionContrib;
import com.example.jobs.Step5_FinalSimilarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DirtDriver extends Configured implements Tool {

    private static void deleteIfExists(Configuration conf, Path p) throws Exception {
        FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) fs.delete(p, true);
    }

    private static void usage() {
        System.err.println(
            "Usage:\n" +
            "  hadoop jar <jar> com.example.DirtDriver <input> <workDir> <positive> <negative> <reducers>\n\n" +
            "Notes:\n" +
            "  - <input> can be a single path OR multiple paths separated by commas.\n" +
            "    Example: s3a://b1/p/,s3a://b2/p/,s3a://b3/p/\n\n" +
            "Example:\n" +
            "  hadoop jar target/dsp2-1.0.0.jar com.example.DirtDriver \\\n" +
            "    s3a://BUCKET/input/biarcs/ s3a://BUCKET/output/run1/ \\\n" +
            "    s3a://BUCKET/test/positive.txt s3a://BUCKET/test/negative.txt 50\n"
        );
    }

    @Override
    public int run(String[] rawArgs) throws Exception {
        java.util.List<String> args = new java.util.ArrayList<>();
        for (String a : rawArgs) {
            if (a != null) {
                a = a.trim();
                if (!a.isEmpty()) args.add(a);
            }
        }

        if (args.size() < 5) {
            usage();
            return 1;
        }

        int n = args.size();

        String inputStr    = args.get(n - 5);
        String workDirStr  = args.get(n - 4);
        String positiveStr = args.get(n - 3);
        String negativeStr = args.get(n - 2);
        String reducersStr = args.get(n - 1);

        int reducers;
        try {
            reducers = Integer.parseInt(reducersStr);
        } catch (Exception e) {
            System.err.println("ERROR: reducers must be an integer, but got: " + reducersStr);
            usage();
            return 1;
        }

        Configuration conf = getConf();

        Path workDir  = new Path(workDirStr);
        Path positive = new Path(positiveStr);
        Path negative = new Path(negativeStr);

        // outputs (5 steps)
        Path out1 = new Path(workDir, "step1_totals");  // Sequence
        Path out2 = new Path(workDir, "step2_mi");      // Text
        Path out3 = new Path(workDir, "step3_denom");   // Sequence
        Path out4 = new Path(workDir, "step4_pairs");   // Sequence
        Path out5 = new Path(workDir, "step5_final");   // Text

        deleteIfExists(conf, out1);
        deleteIfExists(conf, out2);
        deleteIfExists(conf, out3);
        deleteIfExists(conf, out4);
        deleteIfExists(conf, out5);

        // ------------------------------------------------------------
        // INPUTS: single path OR comma-separated list of paths
        // ------------------------------------------------------------
        String[] rawInputs = inputStr.split(",");
        java.util.List<Path> inputPaths = new java.util.ArrayList<>();

        for (String s : rawInputs) {
            if (s == null) continue;
            s = s.trim();
            if (!s.isEmpty()) inputPaths.add(new Path(s));
        }

        if (inputPaths.isEmpty()) {
            System.err.println("ERROR: empty input");
            usage();
            return 1;
        }

        // Step 1: Extract + Totals (SequenceFile output)
        Job j1 = Step1_ExtractAndTotals.buildJob(conf, inputPaths.get(0), out1, reducers);
        for (int i = 1; i < inputPaths.size(); i++) {
            FileInputFormat.addInputPath(j1, inputPaths.get(i));
        }
        if (!j1.waitForCompletion(true)) return 2;

        // Step 2: Compute MI (Sequence -> Text)
        Job j2 = Step2_ComputeMI.buildJob(conf, out1, out2, positive, negative, reducers);
        if (!j2.waitForCompletion(true)) return 3;

        // Step 3: Compute Denom (Text -> Sequence)
        Job j3 = Step3_ComputeDenom.buildJob(conf, out2, out3, positive, negative, reducers);
        if (!j3.waitForCompletion(true)) return 4;

        // Step 4: Intersection Contrib (Text -> Sequence)
        Job j4 = Step4_IntersectionContrib.buildJob(conf, out2, out4, positive, negative, reducers);
        if (!j4.waitForCompletion(true)) return 5;

        // Step 5: Final Similarity (Sequence + denom Sequence -> Text)
        Job j5 = Step5_FinalSimilarity.buildJob(conf, out4, out3, out5, positive, negative, reducers);
        if (!j5.waitForCompletion(true)) return 6;

        System.out.println("DONE. Final output at: " + out5);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new DirtDriver(), args);
        System.exit(code);
    }
}
