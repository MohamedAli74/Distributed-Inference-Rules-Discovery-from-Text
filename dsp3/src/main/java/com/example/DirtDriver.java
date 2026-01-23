package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.jobs.Step1_ExtractPredicates;
import com.example.jobs.Step2_ComputeTotals;
import com.example.jobs.Step3_JoinPSWWithPSTotals;
import com.example.jobs.Step4_ComputeMI;
import com.example.jobs.Step5_ComputeDenom;
import com.example.jobs.Step6_IntersectionContrib;
import com.example.jobs.Step7_FinalSimilarity;

public class DirtDriver extends Configured implements Tool {

    private static void deleteIfExists(Configuration conf, Path p) throws Exception {
        FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) fs.delete(p, true);
    }

    private static void ensureNotExists(Configuration conf, Path p) throws Exception {
        deleteIfExists(conf, p);
    }

    /**
     * Merge Job 7 (final similarity) with Job 4 MI text into a single final output folder
     */
    private static void mergeFinalOutput(Configuration conf, Path out4, Path out7, Path finalMergeDir) throws Exception {
        FileSystem fs = finalMergeDir.getFileSystem(conf);
        deleteIfExists(conf, finalMergeDir);
        fs.mkdirs(finalMergeDir);

        System.out.println("Merging final output: Job 7 (similarity) + Job 4 (text-r-* MI files)...");

        // Copy Job 7 output (final similarity)
        for (FileStatus file : fs.listStatus(out7)) {
            if (!file.getPath().getName().startsWith("_")) {
                fs.copyToLocalFile(file.getPath(), new Path(finalMergeDir.toString() + "/" + file.getPath().getName()));
            }
        }

        // Copy Job 4 MI text output (text-r-* files only)
        for (FileStatus file : fs.listStatus(out4)) {
            String name = file.getPath().getName();
            if (name.startsWith("text-r-")) {
                String destName = "mi-" + name;
                fs.copyToLocalFile(file.getPath(), new Path(finalMergeDir.toString() + "/" + destName));
            }
        }

        System.out.println("Final merged output saved to: " + finalMergeDir);
    }

    private static void usage() {
        System.err.println(
            "Usage:\n" +
            "  hadoop jar <jar> com.example.DirtDriver <input> <workDir> <positive> <negative> <reducers>\n\n" +
            "Examples:\n" +
            "  hadoop jar target/dsp2-1.0.0.jar com.example.DirtDriver \\\n" +
            "    s3a://BUCKET/input/bircs/ s3a://BUCKET/output/run1/ \\\n" +
            "    s3a://BUCKET/test/positive-preds.txt s3a://BUCKET/test/negative-preds.txt 50\n"
        );
    }

    @Override
    public int run(String[] rawArgs) throws Exception {
        java.util.List<String> args = new java.util.ArrayList<>();
        for (String a : rawArgs) {
            System.out.println("ARG: '" + a + "'");
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
        String input = args.get(0);
        String workDirStr  = args.get(1);
        String positiveStr = args.get(2);
        String negativeStr = args.get(3);
        String reducersStr = args.get(4);

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
        Path out4Seq = new Path(out4, "sequence");
        Path out4Text = new Path(out4, "text");

        Path out5 = new Path(workDir, "step5");
        Path out6 = new Path(workDir, "step6");
        Path out7 = new Path(workDir, "step7");
        Path finalOutput = new Path(workDir, "final");

        ensureNotExists(conf, out1);
        ensureNotExists(conf, out2);
        ensureNotExists(conf, out3);
        ensureNotExists(conf, out4);
        ensureNotExists(conf, out5);
        ensureNotExists(conf, out6);
        ensureNotExists(conf, out7);

        Job j1 = Step1_ExtractPredicates.buildJob(conf, input, out1, reducers);
        if (!j1.waitForCompletion(true)) return 2;

        Job j2 = Step2_ComputeTotals.buildJob(conf, out1, out2, reducers);
        if (!j2.waitForCompletion(true)) return 3;

        Job j3 = Step3_JoinPSWWithPSTotals.buildJob(conf, out1, out2, out3, reducers);
        if (!j3.waitForCompletion(true)) return 4;

        Job j4 = Step4_ComputeMI.buildJob(conf, out3, out2, out4, reducers);
        if (!j4.waitForCompletion(true)) return 5;

        Job j5 = Step5_ComputeDenom.buildJob(conf, out4Seq, out5, positive, negative, reducers);
        if (!j5.waitForCompletion(true)) return 6;

        Job j6 = Step6_IntersectionContrib.buildJob(conf, out4Seq, out6, positive, negative, reducers);
        if (!j6.waitForCompletion(true)) return 7;

        Job j7 = Step7_FinalSimilarity.buildJob(conf, out6, out5, out7, positive, negative, reducers);
        if (!j7.waitForCompletion(true)) return 8;

        // Merge final output: Job 7 similarity + Job 4 text files (text-r-*)
        mergeFinalOutput(conf, out4Text, out7, finalOutput);

        System.out.println("DONE. Final output at: " + finalOutput);
        return 0;
    }


    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new DirtDriver(), args);
        System.exit(code);
    }
}