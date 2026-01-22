package com.example;

import com.example.jobs.Step1_ExtractPredicates;
import com.example.jobs.Step2_ComputeTotals;
import com.example.jobs.Step3_JoinPSWWithPSTotals;
import com.example.jobs.Step4_ComputeMI;
import com.example.jobs.Step5_ComputeDenom;
import com.example.jobs.Step6_IntersectionContrib;
import com.example.jobs.Step7_FinalSimilarity;

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

    private static void ensureNotExists(Configuration conf, Path p) throws Exception {
        deleteIfExists(conf, p);
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

        System.out.println("ARGS = " + args);

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

        Path out1 = new Path(workDir, "step1");
        Path out2 = new Path(workDir, "step2");
        Path out3 = new Path(workDir, "step3");
        Path out4 = new Path(workDir, "step4");
        Path out5 = new Path(workDir, "step5");
        Path out6 = new Path(workDir, "step6");
        Path out7 = new Path(workDir, "step7");

        ensureNotExists(conf, out1);
        ensureNotExists(conf, out2);
        ensureNotExists(conf, out3);
        ensureNotExists(conf, out4);
        ensureNotExists(conf, out5);
        ensureNotExists(conf, out6);
        ensureNotExists(conf, out7);

        // ------------------------------------------------------------
        // INPUTS: single path OR comma-separated list of paths
        // ------------------------------------------------------------
        String[] inputs = inputStr.split(",");
        if (inputs.length == 0) {
            System.err.println("ERROR: empty input");
            usage();
            return 1;
        }

        Path firstInput = new Path(inputs[0].trim());

        // Step 1
        Job j1 = Step1_ExtractPredicates.buildJob(conf, firstInput, out1, reducers);

        // Add remaining input paths to the same Job (Step1)
        for (int i = 1; i < inputs.length; i++) {
            String s = inputs[i].trim();
            if (!s.isEmpty()) {
                FileInputFormat.addInputPath(j1, new Path(s));
            }
        }

        if (!j1.waitForCompletion(true)) return 2;

        // Step 2
        Job j2 = Step2_ComputeTotals.buildJob(conf, out1, out2, reducers);
        if (!j2.waitForCompletion(true)) return 3;

        // Step 3
        Job j3 = Step3_JoinPSWWithPSTotals.buildJob(conf, out1, out2, out3, reducers);
        if (!j3.waitForCompletion(true)) return 4;

        // Step 4
        Job j4 = Step4_ComputeMI.buildJob(conf, out3, out2, out4, reducers);
        if (!j4.waitForCompletion(true)) return 5;

        // Step 5
        Job j5 = Step5_ComputeDenom.buildJob(conf, out4, out5, positive, negative, reducers);
        if (!j5.waitForCompletion(true)) return 6;

        // Step 6
        Job j6 = Step6_IntersectionContrib.buildJob(conf, out4, out6, positive, negative, reducers);
        if (!j6.waitForCompletion(true)) return 7;

        // Step 7
        Job j7 = Step7_FinalSimilarity.buildJob(conf, out6, out5, out7, positive, negative, reducers);
        if (!j7.waitForCompletion(true)) return 8;

        System.out.println("DONE. Final output at: " + out7);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new DirtDriver(), args);
        System.exit(code);
    }
}
