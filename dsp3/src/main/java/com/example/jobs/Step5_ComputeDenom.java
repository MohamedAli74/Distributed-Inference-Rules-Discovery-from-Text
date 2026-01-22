package com.example.jobs;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 * Step5:
 * Input (TEXT): lines from Step4.
 *   Supported formats:
 *     A) pred \t slot \t word \t mi
 *     B) pred \t slot \t word \t mi \t   (trailing tab / empty value)
 *     C) (key/value text output) pred \t slot \t word \t mi   (still works)
 *
 * Output (SequenceFile): key = pred , value = denom (sum of positive MI)
 */
public class Step5_ComputeDenom {

    public static class DenomMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        private Set<String> testPreds;
        private final PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] files = ctx.getCacheFiles();
            testPreds = TestData.loadTestPredicates(files, stemmer);
        }

        private static String lastNonEmpty(String[] arr) {
            for (int i = arr.length - 1; i >= 0; i--) {
                String s = arr[i];
                if (s != null) {
                    s = s.trim();
                    if (!s.isEmpty()) return s;
                }
            }
            return null;
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Split keeping empties to tolerate trailing tabs
            String[] parts = line.split("\t", -1);

            String pred = null;
            String miStr = null;

            // Most common: pred slot word mi
            if (parts.length >= 4) {
                pred = parts[0].trim();
                miStr = lastNonEmpty(parts); // MI should be last meaningful token
            } else if (parts.length == 2) {
                // Sometimes TextOutputFormat prints: "<key>\t<value>"
                // where <key> might be "pred\tslot\tword" and <value> is mi
                String[] k = parts[0].split("\t", -1);
                if (k.length == 3) {
                    pred = k[0].trim();
                    miStr = parts[1].trim();
                } else {
                    return;
                }
            } else {
                return;
            }

            if (pred == null || pred.isEmpty() || miStr == null || miStr.isEmpty()) return;

            double mi;
            try {
                mi = Double.parseDouble(miStr);
            } catch (Exception e) {
                return;
            }

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

        // ✅ Input TEXT (from Step4)
        job.setInputFormatClass(TextInputFormat.class);

        // ✅ Output SequenceFile (as required)
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Fix Wrong FS: take FS from each path (s3a/hdfs) correctly
        FileSystem fsPos = positive.getFileSystem(conf);
        FileSystem fsNeg = negative.getFileSystem(conf);

        Path fullPositivePath = fsPos.makeQualified(positive);
        Path fullNegativePath = fsNeg.makeQualified(negative);

        // enable symlink names (positive.txt / negative.txt) if your loader expects them
        job.getConfiguration().setBoolean("mapreduce.job.cache.symlink.create", true);

        job.addCacheFile(new URI(fullPositivePath.toString() + "#positive.txt"));
        job.addCacheFile(new URI(fullNegativePath.toString() + "#negative.txt"));

        FileInputFormat.addInputPath(job, miInput);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
