package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import java.util.*;

/**
 * Step6:
 * Input: Step4 output (sequence files: sequence-r-00000, sequence-r-00001, etc.):
 *   key: pred \t slot \t word
 *   value: mi
 *
 * Output:
 *   key: pairKey
 *   value: contrib
 *
 * Where:
 *  pairKey = canonicalPairKey(pred1, pred2)
 *  contrib accumulates numerator pieces:
 *    sum over shared features f of (MI(pred1,f) + MI(pred2,f))
 *
 * We only emit pairs that exist in test set (positive/negative).
 */
public class Step6_IntersectionContrib {

    /** Filter to only read sequence-r-* files from Step4 output */
    public static class SequenceFilePathFilter implements PathFilter {
        @Override
        public boolean accept(Path p) {
            String name = p.getName();
            return name.startsWith("sequence-r-");
        }
    }

    public static class ContribMapper extends Mapper<Text, DoubleWritable, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, DoubleWritable value, Context ctx) throws IOException, InterruptedException {
            // key pred \t slot \t word ,value: mi
            String[] f = key.toString().split("\t");
            if (f.length != 3) return;

            String pred = f[0];
            String slot = f[1];
            String word = f[2];

            double mi = value.get();

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
                String[] p = t.toString().split("\t");
                if (p.length != 2) continue;
                preds.add(p[0]);
                try { mis.add(Double.parseDouble(p[1])); }
                catch (Exception e) { mis.add(0.0); }
            }

            int n = preds.size();
            if (n < 2) return;

            for (int i = 0; i < n; i++) {
                String p1 = preds.get(i);
                double mi1 = mis.get(i);
                if (mi1 <= 0) continue;

                for (int j = i + 1; j < n; j++) {
                    String p2 = preds.get(j);
                    double mi2 = mis.get(j);
                    if (mi2 <= 0) continue;

                    String pairKey = TestData.canonicalPairKey(p1, p2);
                    if (!allowedPairs.contains(pairKey)) continue;

                    double contrib = mi1 + mi2;
                    outVal.set(contrib);
                    ctx.write(new Text(pairKey), outVal);
                }
            }
        }
    }

    public static Job buildJob(Configuration conf, Path miInput, Path output,
                               Path positive, Path negative, int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step6-IntersectionContrib");
        job.setJarByClass(Step6_IntersectionContrib.class);

        job.setMapperClass(ContribMapper.class);
        job.setReducerClass(ContribReducer.class);
        job.setNumReduceTasks(reducers);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // add test files to cache
        job.addCacheFile(new URI(positive.toString() + "#positive.txt"));
        job.addCacheFile(new URI(negative.toString() + "#negative.txt"));

        // Use PathFilter to only read sequence-r-* files from Step4
        FileInputFormat.setInputPathFilter(job, SequenceFilePathFilter.class);
        
        FileInputFormat.addInputPath(job, miInput);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
