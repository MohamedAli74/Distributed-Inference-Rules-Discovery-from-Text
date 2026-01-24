package com.example.jobs;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

/**
 * Step7:
 * Inputs:
 *  A) Step6 output:
 *     key: p1 \t p2 
 *     value: contrib
 *
 *  B) Step4 output (from 'sequence' subdirectory):
 *    key: pred \t slot \t word
 *    value: mi
 * 
 *  Saved as a table inside the reducer:-
 *  Step5 output:
 *     key: pred
 *     value: denom
 *
 * Output:
 *   pred1 \t pred2 \t similarity \t label
 *
 * Notes:
 * - pairKey is canonical (predA + SEP + predB)
 * - label is 1 if in positive file, 0 if in negative file
 */
public class Step7_FinalSimilarity {

    public static class FinalMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void map(Text key, DoubleWritable value, Context ctx) throws IOException, InterruptedException {
            // Step6 line: pairKey \t contrib
            String[] f = key.toString().split("\t");
            if (f.length == 2){
                double contrib = value.get();

                outKey.set(f[0]+"\t"+f[1]);// p1\tp2
                outVal.set(contrib);
                ctx.write(outKey, outVal);
            }
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
                if (st.getPath().getName().startsWith("_")) continue;

                //Open a SequenceFile Reader
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()));

                // Step 5 output was: Key=Text, Value=DoubleWritable, we use dynamic instantiation
                Writable key = (Writable) org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Writable val = (Writable) org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, val)) {
                    String pred = key.toString();
                    
                    double d = 0.0;
                    if (val instanceof DoubleWritable) {
                        d = ((DoubleWritable) val).get();
                    } else {
                        // Fallback if it was written as Text
                        try { d = Double.parseDouble(val.toString()); } catch (Exception e) { continue; }
                    }

                    denomMap.put(pred, d);
                }
                reader.close();
            }
        }

        @Override
        protected void reduce(Text pairKeyTxt, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            if (pairKeyTxt.toString().startsWith("*")) {
                String[] f = pairKeyTxt.toString().substring(1).split("\t");
                ctx.write(new Text(f[0]+"\t"+f[1]+"\t"+f[2]), new Text(vals.iterator().next().toString()));
                return;
            }
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

        // Input: Step6 output only
        MultipleInputs.addInputPath(job, step6Input, SequenceFileInputFormat.class, FinalMapper.class);
        
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}


