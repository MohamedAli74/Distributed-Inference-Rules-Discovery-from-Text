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
 * Step3: ComputeDenom
 *
 * Input (TEXT): lines from MI output (your Step2_ComputeMI).
 * Expected common format:
 *   pred \t slot \t word \t mi
 *
 * Also tolerates:
 *   pred \t slot \t word \t mi \t        (trailing tab / empty token)
 *   <key> \t <value>                     (rare "key/value" text output)
 *
 * Output (SequenceFile):
 *   key   = pred
 *   value = denom = sum of positive MI values for that predicate
 */
public class Step3_ComputeDenom {

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
            // DO NOT trim() the whole line, because it can delete trailing tabs we want to tolerate.
            String line = value.toString();
            if (line == null) return;

            // Handle Windows CRLF if present
            if (line.indexOf('\r') >= 0) line = line.replace("\r", "");

            // If the line is truly empty/whitespace, skip
            if (line.length() == 0 || line.trim().isEmpty()) return;

            // Split keeping empties to tolerate trailing tabs
            String[] parts = line.split("\t", -1);

            String pred = null;
            String miStr = null;

            // Common: pred \t slot \t word \t mi (or with trailing tabs)
            if (parts.length >= 4) {
                pred = parts[0] == null ? null : parts[0].trim();
                miStr = lastNonEmpty(parts); // MI should be last meaningful token
            }
            // Fallback: sometimes TextOutputFormat prints "<key>\t<value>"
            // If separator was non-tab, or key had no tabs, you might see 2 fields.
            else if (parts.length == 2) {
                String keyPart = parts[0] == null ? "" : parts[0].trim();
                String valPart = parts[1] == null ? "" : parts[1].trim();

                if (keyPart.isEmpty() || valPart.isEmpty()) return;

                // keyPart might be "pred\tslot\tword" or just "pred"
                String[] k = keyPart.split("\t", -1);
                pred = (k.length >= 1) ? k[0].trim() : null;
                miStr = valPart;
            } else {
                return;
            }

            if (pred == null || pred.isEmpty() || miStr == null || miStr.isEmpty()) return;

            // Filter only predicates that exist in test sets (keeps denom small & matches later steps)
            if (testPreds != null && !testPreds.contains(pred)) return;

            double mi;
            try {
                mi = Double.parseDouble(miStr);
            } catch (Exception e) {
                ctx.getCounter("Step3", "BAD_MI_PARSE").increment(1);
                return;
            }

            // denom sums ONLY positive MI
            if (mi <= 0.0) return;

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

    public static Job buildJob(Configuration conf,
                               Path miInput,
                               Path output,
                               Path positive,
                               Path negative,
                               int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step3-ComputeDenom");
        job.setJarByClass(Step3_ComputeDenom.class);

        job.setMapperClass(DenomMapper.class);

        // Safe combiner (sum)
        job.setCombinerClass(SumReducer.class);

        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        // Input TEXT
        job.setInputFormatClass(TextInputFormat.class);

        // Output SequenceFile (as required)
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Make cache paths fully-qualified (s3a:// vs hdfs://)
        FileSystem fsPos = positive.getFileSystem(conf);
        FileSystem fsNeg = negative.getFileSystem(conf);

        Path fullPositivePath = fsPos.makeQualified(positive);
        Path fullNegativePath = fsNeg.makeQualified(negative);

        // Enable symlink names (positive.txt / negative.txt) if your loader expects them
        job.getConfiguration().setBoolean("mapreduce.job.cache.symlink.create", true);
        job.addCacheFile(new URI(fullPositivePath.toString() + "#positive.txt"));
        job.addCacheFile(new URI(fullNegativePath.toString() + "#negative.txt"));

        FileInputFormat.addInputPath(job, miInput);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
