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
import java.util.*;

/**
 * Step4: IntersectionContrib
 *
 * Input (TEXT): MI lines from Step2_ComputeMI output:
 *   pred \t slot \t word \t mi
 *
 * Output (SequenceFile):
 *   key: pairKey (pred1 \t pred2)  [canonical]
 *   value: contrib (DoubleWritable)
 *
 * We only emit pairs that exist in test set (positive/negative).
 */
public class Step4_IntersectionContrib {

    /** Mapper: reads TEXT lines from MI output */
    public static class ContribMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

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
            String line = value.toString();
            if (line == null) return;

            // tolerate CRLF
            if (line.indexOf('\r') >= 0) line = line.replace("\r", "");

            // skip empty lines
            if (line.trim().isEmpty()) return;

            // pred \t slot \t word \t mi  (may have trailing tabs)
            String[] parts = line.split("\t", -1);
            if (parts.length < 4) return;

            String pred = parts[0].trim();
            String slot = parts[1].trim();
            String word = parts[2].trim();

            if (pred.isEmpty() || slot.isEmpty() || word.isEmpty()) return;

            String miStr = lastNonEmpty(parts);
            if (miStr == null) return;

            double mi;
            try { mi = Double.parseDouble(miStr); }
            catch (Exception e) { return; }

            if (mi <= 0) return;

            // group by feature f = (slot,word)
            outKey.set(slot + "\t" + word);
            // value: pred \t mi
            outVal.set(pred + "\t" + mi);
            ctx.write(outKey, outVal);
        }
    }

    public static class ContribReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final DoubleWritable outVal = new DoubleWritable();
        private final Text outKey = new Text();

        private final PorterStemmer stemmer = new PorterStemmer();
        private Set<String> allowedPairs;

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] cache = ctx.getCacheFiles();
            Map<String, TestData.PairInfo> pairs = TestData.loadPairs(cache, stemmer);
            allowedPairs = pairs.keySet();
        }

        @Override
        protected void reduce(Text featureKey, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {

            List<String> preds = new ArrayList<>();
            List<Double> mis = new ArrayList<>();

            for (Text t : vals) {
                String[] p = t.toString().split("\t", -1);
                if (p.length != 2) continue;

                String pred = p[0].trim();
                if (pred.isEmpty()) continue;

                double mi;
                try { mi = Double.parseDouble(p[1]); }
                catch (Exception e) { continue; }

                if (mi <= 0) continue;

                preds.add(pred);
                mis.add(mi);
            }

            int n = preds.size();
            if (n < 2) return;

            for (int i = 0; i < n; i++) {
                String p1 = preds.get(i);
                double mi1 = mis.get(i);

                for (int j = i + 1; j < n; j++) {
                    String p2 = preds.get(j);
                    double mi2 = mis.get(j);

                    String pairKey = TestData.canonicalPairKey(p1, p2);
                    if (!allowedPairs.contains(pairKey)) continue;

                    // contribution for this shared feature
                    double contrib = mi1 + mi2;

                    outKey.set(pairKey);
                    outVal.set(contrib);
                    ctx.write(outKey, outVal);
                }
            }
        }
    }

    public static Job buildJob(Configuration conf,
                               Path miInput,
                               Path output,
                               Path positive,
                               Path negative,
                               int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step4-IntersectionContrib");
        job.setJarByClass(Step4_IntersectionContrib.class);

        job.setMapperClass(ContribMapper.class);
        job.setReducerClass(ContribReducer.class);
        job.setNumReduceTasks(reducers);

        // Input = TEXT (MI output)
        job.setInputFormatClass(TextInputFormat.class);

        // Output = SequenceFile (used by Step5/FinalSimilarity)
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // cache test files (pairs are built from positive+negative)
        FileSystem fsPos = positive.getFileSystem(conf);
        FileSystem fsNeg = negative.getFileSystem(conf);

        Path fullPositivePath = fsPos.makeQualified(positive);
        Path fullNegativePath = fsNeg.makeQualified(negative);

        job.getConfiguration().setBoolean("mapreduce.job.cache.symlink.create", true);
        job.addCacheFile(new URI(fullPositivePath.toString() + "#positive.txt"));
        job.addCacheFile(new URI(fullNegativePath.toString() + "#negative.txt"));

        FileInputFormat.addInputPath(job, miInput);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
