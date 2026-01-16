package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Step7:
 * Inputs:
 *  A) Step6 output:
 *     pairKey \t contrib
 *
 *  B) Step5 output:
 *     pred \t denom
 *
 * Output:
 *   pred1 \t pred2 \t similarity \t label
 *
 * Notes:
 * - pairKey is canonical (predA + SEP + predB)
 * - label is 1 if in positive file, 0 if in negative file
 */
public class Step7_FinalSimilarity {

    public static class FinalMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step6 line: pairKey \t contrib
            String[] f = value.toString().split("\t");
            if (f.length != 2) return;

            double contrib;
            try { contrib = Double.parseDouble(f[1]); }
            catch (Exception e) { return; }

            outKey.set(f[0]);      // pairKey
            outVal.set(contrib);
            ctx.write(outKey, outVal);
        }
    }

    public static class FinalReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private final PorterStemmer stemmer = new PorterStemmer();
        private Map<String, Double> denomMap = new HashMap<>();
        private Map<String, TestData.PairInfo> testPairs = new HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();

            // 1) load denomMap from Step5 output dir
            String denomDir = conf.get("dirt.denom.dir");
            if (denomDir != null) {
                loadDenoms(conf, new Path(denomDir));
            }

            // 2) load test pairs (for label + original p1,p2)
            URI[] cache = ctx.getCacheFiles();
            testPairs = TestData.loadPairs(cache, stemmer);
        }

        private void loadDenoms(Configuration conf, Path denomDir) throws IOException {
            FileSystem fs = denomDir.getFileSystem(conf);
            if (!fs.exists(denomDir)) return;

            for (FileStatus st : fs.listStatus(denomDir)) {
                if (!st.isFile()) continue;
                String name = st.getPath().getName();
                if (!name.startsWith("part-")) continue;

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(st.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // line: pred \t denom
                        String[] f = line.split("\t");
                        if (f.length != 2) continue;

                        String pred = f[0];
                        double d;
                        try { d = Double.parseDouble(f[1]); }
                        catch (Exception e) { continue; }

                        denomMap.put(pred, d);
                    }
                }
            }
        }

        @Override
        protected void reduce(Text pairKeyTxt, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {

            double num = 0.0;
            for (DoubleWritable v : vals) num += v.get();

            String pairKey = pairKeyTxt.toString();
            TestData.PairInfo info = testPairs.get(pairKey);
            if (info == null) {
                return;
            }

            double d1 = denomMap.getOrDefault(info.p1, 0.0);
            double d2 = denomMap.getOrDefault(info.p2, 0.0);
            double denom = d1 + d2;

            double sim = 0.0;
            if (denom > 0.0) sim = num / denom;

            // output: p1 \t p2 \t sim \t label
            String out = sim + "\t" + info.label;
            ctx.write(new Text(info.p1 + "\t" + info.p2), new Text(out));
        }
    }

    public static Job buildJob(Configuration conf, Path step6Input, Path denomDir, Path output,
                               Path positive, Path negative, int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step7-FinalSimilarity");
        job.setJarByClass(Step7_FinalSimilarity.class);

        // pass denom dir to reducer
        job.getConfiguration().set("dirt.denom.dir", denomDir.toString());

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add test files (for label)
        job.addCacheFile(new URI(positive.toString() + "#positive.txt"));
        job.addCacheFile(new URI(negative.toString() + "#negative.txt"));

        TextInputFormat.addInputPath(job, step6Input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
