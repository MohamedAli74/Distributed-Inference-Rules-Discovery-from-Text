package com.example.jobs;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 * Step5:
 * Input (SequenceFile): key = "pred\tslot\tword" , value = mi
 *   - Reads from Step4 output (sequence files: sequence-r-00000, sequence-r-00001, etc.)
 * Output (SequenceFile): key = pred , value = denom
 */
public class Step5_ComputeDenom {

    /** Filter to only read sequence-r-* files from Step4 output */
    public static class SequenceFilePathFilter implements PathFilter {
        @Override
        public boolean accept(Path p) {
            String name = p.getName();
            return name.startsWith("sequence-r-");
        }
    }

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
        protected void map(Text key, DoubleWritable value, Context ctx) throws IOException, InterruptedException {
            // key is: pred \t slot \t word
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

        // Keep SequenceFile
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //  Fix Wrong FS: take FS from each path (s3a/hdfs) correctly
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
