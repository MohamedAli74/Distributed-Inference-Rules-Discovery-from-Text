package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Step7:
 * Input:
 *  A) Step6 output (SequenceFile):
 *     key: p1 \t p2
 *     value: contrib
 *
 * Loads Step5 denom table from denomDir (SequenceFile) in reducer setup.
 *
 * Output (TEXT):
 *   pred1 \t pred2 \t similarity \t label
 *
 * NOTE: This version DOES NOT read Step4 at all (no MI printing).
 */
public class Step7_FinalSimilarity {

    /** Mapper: accepts ONLY Step6 records */
    public static class FinalMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void map(Text key, DoubleWritable value, Context ctx) throws IOException, InterruptedException {
            // Step6: key = "p1\tp2", value = contrib
            String[] f = key.toString().split("\t", -1);
            if (f.length != 2) return;
            ctx.write(new Text(f[0] + "\t" + f[1]), value);
        }
    }

    public static class FinalReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private final PorterStemmer stemmer = new PorterStemmer();
        private final Map<String, Double> denomMap = new HashMap<>();
        private Map<String, TestData.PairInfo> testPairs = new HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();

            // 1) load denomMap from Step5 output dir (SequenceFile)
            String denomDir = conf.get("dirt.denom.dir");
            if (denomDir != null) {
                loadDenoms(conf, new Path(denomDir));
            }

            // 2) load test pairs (label + original p1,p2)
            URI[] cache = ctx.getCacheFiles();
            testPairs = TestData.loadPairs(cache, stemmer);
        }

        private void loadDenoms(Configuration conf, Path denomDir) throws IOException {
            FileSystem fs = denomDir.getFileSystem(conf);
            if (!fs.exists(denomDir)) return;

            for (FileStatus st : fs.listStatus(denomDir)) {
                if (st.getPath().getName().startsWith("_")) continue;

                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()));

                Writable key = (Writable) org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Writable val = (Writable) org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, val)) {
                    String pred = key.toString();
                    double d;
                    try { d = Double.parseDouble(val.toString()); }
                    catch (Exception e) { continue; }
                    denomMap.put(pred, d);
                }
                reader.close();
            }
        }

        @Override
        protected void reduce(Text pairKeyTxt, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {

            // sum numerator contributions
            double num = 0.0;
            for (DoubleWritable v : vals) num += v.get();

            String pairKey = pairKeyTxt.toString();

            // output only pairs that exist in testPairs
            TestData.PairInfo info = testPairs.get(pairKey);
            if (info == null) return;

            double d1 = denomMap.getOrDefault(info.p1, 0.0);
            double d2 = denomMap.getOrDefault(info.p2, 0.0);
            double denom = d1 + d2;

            double sim = 0.0;
            if (denom > 0.0) sim = num / denom;

            // if you REALLY want "only similar" you can uncomment:
            // if (sim <= 0.0) return;

            ctx.write(new Text(info.p1 + "\t" + info.p2),
                      new Text(sim + "\t" + info.label));
        }
    }

    public static Job buildJob(Configuration conf,
                               Path step6Input,
                               Path denomDir,
                               Path output,
                               Path positive,
                               Path negative,
                               int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step7-FinalSimilarity");
        job.setJarByClass(Step7_FinalSimilarity.class);

        job.getConfiguration().set("dirt.denom.dir", denomDir.toString());

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, output);

        // cache files for labels
        job.getConfiguration().setBoolean("mapreduce.job.cache.symlink.create", true);
        job.addCacheFile(new URI(positive.toString() + "#positive.txt"));
        job.addCacheFile(new URI(negative.toString() + "#negative.txt"));

        MultipleInputs.addInputPath(job, step6Input, SequenceFileInputFormat.class, FinalMapper.class);

        return job;
    }
}
