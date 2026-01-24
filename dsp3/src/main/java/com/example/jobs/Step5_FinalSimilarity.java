package com.example.jobs;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Step5: FinalSimilarity
 *
 * Input:
 *  Step4 output (SequenceFile):
 *     key: p1 \t p2          (canonical)
 *     value: contrib (DoubleWritable)
 *
 * Also loads denom table from denomDir (SequenceFile) in reducer setup.
 *
 * Output (TEXT):
 *   pred1 \t pred2 \t similarity \t label
 *
 * IMPORTANT:
 *  - prints ALL test pairs from positive+negative files (keeps duplicates and orientation).
 *  - if a pair never appears in Step4, it will still be printed with similarity=0.0
 */
public class Step5_FinalSimilarity {

    /** Mapper: passthrough Step4 records */
    public static class FinalMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void map(Text key, DoubleWritable value, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, value);
        }
    }

    public static class FinalReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private final PorterStemmer stemmer = new PorterStemmer();

        // denom[p] from Step3
        private final Map<String, Double> denomMap = new HashMap<>();

        // all test pairs (positive+negative) keyed by canonical "p1\tp2"
        // value keeps duplicates + orientation
        private Map<String, List<TestData.PairInfo>> testPairs = new HashMap<>();

        // which canonical pairs we already printed (those that arrived from Step4)
        private final Set<String> printedPairs = new HashSet<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();

            // 1) load denomMap from Step3 output dir (SequenceFile)
            String denomDir = conf.get("dirt.denom.dir");
            if (denomDir != null) {
                loadDenoms(conf, new Path(denomDir));
            }

            // 2) load test pairs with DUPLICATES (and labels)
            URI[] cache = ctx.getCacheFiles();
            testPairs = TestData.loadPairsWithDuplicates(cache, stemmer);
        }

        private void loadDenoms(Configuration conf, Path denomDir) throws IOException {
            FileSystem fs = denomDir.getFileSystem(conf);
            if (!fs.exists(denomDir)) return;

            FileStatus[] statuses = fs.listStatus(denomDir);
            if (statuses == null) return;

            Text k = new Text();
            DoubleWritable v = new DoubleWritable();

            for (FileStatus st : statuses) {
                String name = st.getPath().getName();
                if (name.startsWith("_") || name.startsWith(".")) continue; // _SUCCESS etc.

                try (SequenceFile.Reader reader =
                             new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()))) {
                    while (reader.next(k, v)) {
                        denomMap.put(k.toString(), v.get());
                    }
                } catch (Exception ignored) {
                    // tolerate non-seq files if exist
                }
            }
        }

        private static String toCanonicalKey(String rawKey) {
            if (rawKey == null) return "";
            String[] p = rawKey.split("\t", -1);
            if (p.length >= 2) return TestData.canonicalPairKey(p[0], p[1]);
            return rawKey;
        }

        @Override
        protected void reduce(Text pairKeyTxt, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {

            String rawKey = pairKeyTxt.toString();
            String canonKey = toCanonicalKey(rawKey);

            // only pairs in test set
            List<TestData.PairInfo> infos = testPairs.get(canonKey);
            if (infos == null || infos.isEmpty()) return;

            // sum numerator contributions
            double num = 0.0;
            for (DoubleWritable v : vals) num += v.get();

            // denom uses normalized predicates from PairInfo
            TestData.PairInfo any = infos.get(0);
            double d1 = denomMap.getOrDefault(any.p1, 0.0);
            double d2 = denomMap.getOrDefault(any.p2, 0.0);
            double denom = d1 + d2;

            double sim = 0.0;
            if (denom > 0.0) sim = num / denom;

            printedPairs.add(canonKey);

            // print ALL duplicates (preserve direction)
            for (TestData.PairInfo info : infos) {
                // key: "p1\tp2", value: "sim\tlabel"  => output has 4 columns
                ctx.write(new Text(info.p1 + "\t" + info.p2),
                          new Text(sim + "\t" + info.label));
            }
        }

        /**
         * Print missing test pairs with similarity=0.0.
         * Works even with multiple reducers: each reducer prints only the keys that hash to it.
         */
        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            Configuration conf = ctx.getConfiguration();
            int numReduces = conf.getInt("mapreduce.job.reduces", 1);

            int myReduceId = 0;
            try {
                myReduceId = ctx.getTaskAttemptID().getTaskID().getId(); // 0..numReduces-1
            } catch (Exception ignored) {}

            for (Map.Entry<String, List<TestData.PairInfo>> e : testPairs.entrySet()) {
                String canonKey = e.getKey();
                if (printedPairs.contains(canonKey)) continue;

                // partition responsibility if reducers > 1 (default HashPartitioner behavior)
                if (numReduces > 1) {
                    int bucket = (canonKey.hashCode() & Integer.MAX_VALUE) % numReduces;
                    if (bucket != myReduceId) continue;
                }

                List<TestData.PairInfo> infos = e.getValue();
                if (infos == null) continue;

                for (TestData.PairInfo info : infos) {
                    ctx.write(new Text(info.p1 + "\t" + info.p2),
                              new Text("0.0\t" + info.label));
                }
            }
        }
    }

    public static Job buildJob(Configuration conf,
                               Path step4Input,
                               Path denomDir,
                               Path output,
                               Path positive,
                               Path negative,
                               int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step5-FinalSimilarity");
        job.setJarByClass(Step5_FinalSimilarity.class);

        job.getConfiguration().set("dirt.denom.dir", denomDir.toString());

        // input: SequenceFile from Step4
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, step4Input);

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        // cache files for labels
        FileSystem fsPos = positive.getFileSystem(conf);
        FileSystem fsNeg = negative.getFileSystem(conf);
        Path fullPositivePath = fsPos.makeQualified(positive);
        Path fullNegativePath = fsNeg.makeQualified(negative);

        job.getConfiguration().setBoolean("mapreduce.job.cache.symlink.create", true);
        job.addCacheFile(new URI(fullPositivePath.toString() + "#positive.txt"));
        job.addCacheFile(new URI(fullNegativePath.toString() + "#negative.txt"));

        return job;
    }
}
