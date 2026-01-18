package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Step 2:
 * Input:  Output of Step1:
 *   predicate \t slot \t word \t count
 *
 * Output:
 *   PS\tpredicate\tslot   \t C(p,slot)
 *   SW\tslot\tword        \t C(slot,word)
 *   SLOT\tslot            \t C(slot)
 *  in MI terms: |p,Slot,*|->count
 *               |*,Slot,w|->count
 *               |*,Slot,*|->count
 */
public class Step2_ComputeTotals {

    public static class TotalsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable outVal = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step1 line: pred \t slot \t word \t count
            //TODO: change the key-value back to <Text, IntWritable>, the same as the previous step output.
            String[] f = value.toString().split("\t");
            if (f.length != 4) return;

            String pred = f[0];
            String slot = f[1];
            String word = f[2];

            int c;
            try { c = Integer.parseInt(f[3]); }//TODO: change here as well
            catch (Exception e) { return; }
            if (c <= 0) return;

            outVal.set(c);

            // 1) PS: C(predicate, slot)
            outKey.set("PS\t" + pred + "\t" + slot);
            ctx.write(outKey, outVal);

            // 2) SW: C(slot, word)
            outKey.set("SW\t" + slot + "\t" + word);
            ctx.write(outKey, outVal);

            // 3) SLOT: C(slot)
            outKey.set("SLOT\t" + slot);
            ctx.write(outKey, outVal);//TODO: make it a global variable
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> vals, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : vals) sum += v.get();
            out.set(sum);
            ctx.write(key, out);
        }
    }

    /** builder للـ Driver */
    public static Job buildJob(Configuration conf, Path input, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step2-ComputeTotals");
        job.setJarByClass(Step2_ComputeTotals.class);

        job.setMapperClass(TotalsMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
