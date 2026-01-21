package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Step 3:
 * Join:
 *  - Step1 output:  key=<pred \t slot \t word>  value=<cpsw>
 *  - Step2 output:  key=<PS \t pred \t slot>    value=<cps>
 *
 * Output (SequenceFile):
 *  key=<pred \t slot \t word>
 *  value=<cpsw \t cps>
 */
public class Step3_JoinPSWWithPSTotals {

    /** Mapper for Step1 output (PSW records) */
    public static class PSWMapper extends Mapper<Text, LongWritable, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            // key: pred \t slot \t word
            String[] k = key.toString().split("\t", -1);
            if (k.length != 3) return;

            String pred = k[0];
            String slot = k[1];
            String word = k[2];

            // join key on (pred,slot)
            outKey.set("J\t" + pred + "\t" + slot);

            // mark as word record: W \t word \t cpsw
            outVal.set("W\t" + word + "\t" + value.get());
            ctx.write(outKey, outVal);
        }
    }

    /** Mapper for Step2 totals output (PS totals only) */
    public static class PSTotalsMapper extends Mapper<Text, LongWritable, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            // key: PS \t pred \t slot
            String ks = key.toString();
            if (!ks.startsWith("PS\t")) return;

            // IMPORTANT: this must yield 3 parts: ["PS", pred, slot]
            String[] p = ks.split("\t", -1);
            if (p.length != 3) return;

            String pred = p[1];
            String slot = p[2];

            outKey.set("J\t" + pred + "\t" + slot);
            outVal.set("T\t" + value.get()); // total cps
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

                if ("T".equals(parts[0])) {
                    // T \t cps
                    cps = parts[1];
                } else if ("W".equals(parts[0]) && parts.length == 3) {
                    // W \t word \t cpsw
                    wordRecs.add(new String[]{parts[1], parts[2]});
                }
            }

            if (cps == null) return;

            // key: J \t pred \t slot
            String[] k = key.toString().split("\t", 3);
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

    public static Job buildJob(Configuration conf, Path step1Input, Path step2Totals, Path output, int reducers)
            throws Exception {

        Job job = Job.getInstance(conf, "Step3-JoinPSW-WithPSTotals");
        job.setJarByClass(Step3_JoinPSWWithPSTotals.class);

        job.setReducerClass(JoinReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Inputs are SequenceFiles from Step1 and Step2
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, step1Input, SequenceFileInputFormat.class, PSWMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, SequenceFileInputFormat.class, PSTotalsMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}