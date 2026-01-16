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
 * Join :
 *  - Step1: pred \t slot \t word \t cpsw
 *  - Step2(PS only): PS \t pred \t slot \t cps
 *
 * Output:
 *  pred \t slot \t word \t cpsw \t cps
 */
public class Step3_JoinPSWWithPSTotals {

    /** Mapper  Step1 */
    public static class PSWMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step1 line: pred \t slot \t word \t cpsw
            String[] f = value.toString().split("\t");
            if (f.length != 4) return;

            String pred = f[0];
            String slot = f[1];
            String word = f[2];
            String cpsw = f[3];

            // join key on (pred,slot)
            outKey.set("J\t" + pred + "\t" + slot);
            // mark as word record
            outVal.set("W\t" + word + "\t" + cpsw);
            ctx.write(outKey, outVal);
        }
    }

    public static class PSTotalsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step2 output: key \t value
            // key 
            // PS\tpred\tslot
            // SW\tslot\tword
            // SLOT\tslot
            String[] kv = value.toString().split("\t");
            if (kv.length != 2) return;

            String k = kv[0];
            String v = kv[1];

            if (!k.startsWith("PS\t")) return;

            // k = PS\tpred\tslot
            String[] p = k.split("\t", 3);
            if (p.length != 3) return;

            String pred = p[1];
            String slot = p[2];
            String cps = v;

            outKey.set("J\t" + pred + "\t" + slot);
            outVal.set("T\t" + cps); // totals record
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
                String[] parts = t.toString().split("\t");
                if (parts.length < 2) continue;

                if (parts[0].equals("T")) {
                    cps = parts[1];
                } else if (parts[0].equals("W") && parts.length == 3) {
                    // W \t word \t cpsw
                    wordRecs.add(new String[]{parts[1], parts[2]});
                }
            }

            if (cps == null) return;

            // key = J\tpred\tslot
            String[] k = key.toString().split("\t", 3);
            if (k.length != 3) return;

            String pred = k[1];
            String slot = k[2];

            for (String[] wc : wordRecs) {
                String word = wc[0];
                String cpsw = wc[1];

                // output:
                // pred \t slot \t word   \t cpsw \t cps
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

        // multiple inputs
        MultipleInputs.addInputPath(job, step1Input, TextInputFormat.class, PSWMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, TextInputFormat.class, PSTotalsMapper.class);

        TextOutputFormat.setOutputPath(job, output);
        return job;
    }
}
