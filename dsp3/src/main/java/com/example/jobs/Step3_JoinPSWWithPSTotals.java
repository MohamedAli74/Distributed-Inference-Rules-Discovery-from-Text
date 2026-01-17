package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Step 3:
 * Join:
 *  - Step1 output lines:  <pred \t slot \t word> \t <cpsw>
 *  - Step2 output lines:  <PS \t pred \t slot> \t <cps>
 *
 * Output:
 *  <pred \t slot \t word> \t <cpsw \t cps>
 */
public class Step3_JoinPSWWithPSTotals {

    /** helper: parse "key \t value" where key may contain tabs */
    private static class KV {
        String key;
        String val;
    }

    private static KV parseLineKV(String line) {
        if (line == null) return null;
        line = line.trim();
        if (line.isEmpty()) return null;

        int lastTab = line.lastIndexOf('\t');
        if (lastTab < 0) return null;

        KV kv = new KV();
        kv.key = line.substring(0, lastTab);
        kv.val = line.substring(lastTab + 1);
        return kv;
    }

    /** Mapper for Step1 output */
    public static class PSWMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step1 line: (pred \t slot \t word) \t cpsw
            KV kv = parseLineKV(value.toString());
            if (kv == null) return;

            String[] k = kv.key.split("\t", -1);
            if (k.length != 3) return;

            String pred = k[0];
            String slot = k[1];
            String word = k[2];
            String cpsw = kv.val;

            // join key on (pred,slot)
            outKey.set("J\t" + pred + "\t" + slot);
            // mark as word record
            outVal.set("W\t" + word + "\t" + cpsw);
            ctx.write(outKey, outVal);
        }
    }

    /** Mapper for Step2 totals output */
    public static class PSTotalsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step2 line: <key> \t <value>
            KV kv = parseLineKV(value.toString());
            if (kv == null) return;

            // we only want PS totals: key starts with "PS\tpred\tslot"
            if (!kv.key.startsWith("PS\t")) return;

            String[] p = kv.key.split("\t", 3);
            if (p.length != 3) return;

            String pred = p[1];
            String slot = p[2];
            String cps = kv.val;

            outKey.set("J\t" + pred + "\t" + slot);
            outVal.set("T\t" + cps);
            ctx.write(outKey, outVal);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            String cps = null;
            List<String[]> wordRecs = new ArrayList<>();

            for (Text t : vals) {
                String[] parts = t.toString().split("\t", -1);
                if (parts.length < 2) continue;

                if (parts[0].equals("T")) {
                    cps = parts[1];
                } else if (parts[0].equals("W") && parts.length == 3) {
                    wordRecs.add(new String[]{parts[1], parts[2]}); // word, cpsw
                }
            }

            if (cps == null) return;

            String[] k = key.toString().split("\t", 3); // J, pred, slot
            if (k.length != 3) return;

            String pred = k[1];
            String slot = k[2];

            for (String[] wc : wordRecs) {
                String word = wc[0];
                String cpsw = wc[1];

                outKey.set(pred + "\t" + slot + "\t" + word);
                outVal.set(cpsw + "\t" + cps);
                ctx.write(outKey, outVal);
            }
        }
    }

    public static Job buildJob(Configuration conf, Path step1Input, Path step2Totals, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step3-JoinPSW-WithPSTotals");
        job.setJarByClass(Step3_JoinPSWWithPSTotals.class);

        job.setReducerClass(JoinReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, step1Input, TextInputFormat.class, PSWMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, TextInputFormat.class, PSTotalsMapper.class);

        TextOutputFormat.setOutputPath(job, output);
        return job;
    }
}
