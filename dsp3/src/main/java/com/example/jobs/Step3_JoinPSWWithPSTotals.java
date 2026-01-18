package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
    /** Mapper for Step1 output */
    public static class PSWMapper extends Mapper<Text, LongWritable, Text, Text> {//TODO: change the key value type
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            String[] k = key.toString().split("\t", -1);
            if (k.length != 3) return;

            String pred = k[0];
            String slot = k[1];
            String word = k[2];

            // join key on (pred,slot)
            outKey.set("J\t" + pred + "\t" + slot);
            // mark as word record
            outVal.set("W\t" + word + "\t" + value.get());
            ctx.write(outKey, outVal);
        }
    }

    /** Mapper for Step2 totals output */
    public static class PSTotalsMapper extends Mapper<Text, LongWritable, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            // we only want PS totals: key starts with "PS\tpred\tslot"
            if (!key.toString().startsWith("PS\t")) return;

            String[] p = key.toString().split("\t", 3);
            if (p.length != 2) return;

            String pred = p[1];
            String slot = p[2];

            outKey.set("J\t" + pred + "\t" + slot);
            outVal.set("T\t" + value.get());
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

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, step1Input, SequenceFileInputFormat.class, PSWMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, SequenceFileInputFormat.class, PSTotalsMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
