package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 * Step5:
 * Input: Step4 output: pred \t slot \t word \t mi
 * Output: pred \t denom
 */
public class Step5_ComputeDenom {

    public static class DenomMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        private Set<String> testPreds;
        private final PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] files = ctx.getCacheFiles();
            testPreds = TestData.loadTestPredicates(files, stemmer);
        }

        @Override
        //filters the input to only test predicates.
        protected void map(Text key, DoubleWritable value, Context ctx) throws IOException, InterruptedException {
            String[] f = key.toString().split("\t");
            if (f.length != 3) return;

            String pred = f[0];
            double mi = value.get();

            if (mi <= 0) return;

            if (testPreds != null && !testPreds.contains(pred)) return;

            outKey.set(pred);
            outVal.set(mi);
            ctx.write(outKey, outVal);
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable out = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> vals, Context ctx) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable v : vals) sum += v.get();
            out.set(sum);
            ctx.write(key, out);
        }
    }

    public static Job buildJob(Configuration conf, Path miInput, Path output,
                               Path positive, Path negative, int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step5-ComputeDenom");
        job.setJarByClass(Step5_ComputeDenom.class);

        job.setMapperClass(DenomMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        
        FileSystem fs = FileSystem.get(conf);
        Path fullPositivePath = fs.makeQualified(positive);
        Path fullNegativePath = fs.makeQualified(negative);

        job.addCacheFile(new URI(fullPositivePath.toString() + "#positive.txt"));
        job.addCacheFile(new URI(fullNegativePath.toString() + "#negative.txt"));
        // ---------------------------------------

        FileInputFormat.addInputPath(job, miInput);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}