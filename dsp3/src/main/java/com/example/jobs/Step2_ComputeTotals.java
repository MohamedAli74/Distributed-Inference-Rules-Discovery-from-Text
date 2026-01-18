package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    public static class TotalsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
        private final Text outKey = new Text();

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            // Input key: predicate \t slot \t word
            // Output value: count
            String[] f = key.toString().split("\t");
            if (f.length != 3) return;

            String pred = f[0];
            String slot = f[1];
            String word = f[2];


            // 1) PS: C(predicate, slot)
            outKey.set("PS\t" + pred + "\t" + slot);
            ctx.write(outKey, value);

            // 2) SW: C(slot, word)
            outKey.set("SW\t" + slot + "\t" + word);
            ctx.write(outKey, value);

            // 3) SLOT: C(slot)
            outKey.set("SLOT\t" + slot);
            ctx.write(outKey, value);
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable out = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : vals) sum += v.get();
            out.set(sum);
            ctx.write(key, out);
        }
    }

    //driver builder
    public static Job buildJob(Configuration conf, Path input, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step2-ComputeTotals");
        job.setJarByClass(Step2_ComputeTotals.class);

        job.setMapperClass(TotalsMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);


        return job;
    }
}
