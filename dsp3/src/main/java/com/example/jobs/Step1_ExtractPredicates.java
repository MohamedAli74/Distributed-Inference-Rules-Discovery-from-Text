package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.example.helpers.Parser;
import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;
import com.example.helpers.Token;
import com.example.helpers.Parser.ParsedLine;
import com.example.helpers.Parser.PredicateInstance;

import java.io.IOException;
import java.util.Optional;

public class Step1_ExtractPredicates {
    /**
     * Input: biarc line - head_word \t syntactic-ngram \t total_count \t counts_by_year  
     * Output: key = predicate \t slot \t word , value = count
     * [in MI terms: |p,Slot,w|->count]
     */

    public static class ExtractMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /** Mapper:
         * Input: biarc line - head_word \t syntactic-ngram \t total_count \t counts_by_year
         * Output: key = predicate \t slot \t word , value = count
         */
        private final Text outKey = new Text();
        private final IntWritable outVal = new IntWritable();
        private final PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            Parser.ParsedLine pl = Parser.parseLine(value.toString());
            if (pl == null || pl.tokens.isEmpty()) return;

            Token root = Parser.findRootVerb(pl.tokens);
            if (root == null) return;

            // skip auxiliary verbs
            if (TestData.isAuxiliary(root.word, stemmer)) return;

            Optional<Parser.PredicateInstance> instOpt = Parser.extractPredicate(pl.tokens, root, stemmer);
            if (!instOpt.isPresent()) return;
            //instOpt -> template (X control Y, X went to Y), xWordStem, yWordStem

            Parser.PredicateInstance inst = instOpt.get();
            int c = pl.count;
            if (c <= 0) return;

            outVal.set(c);

            // (pred, X, xWord)
            outKey.set(inst.template + "\tX\t" + inst.xWordStem);
            ctx.write(outKey, outVal);

            // (pred, Y, yWord)
            outKey.set(inst.template + "\tY\t" + inst.yWordStem);
            ctx.write(outKey, outVal);
        }
    }

    /** Reducer: sum counts 
     * Input: key = predicate \t slot \t word , value = count
     * Output: same as input, but summed counts
    */
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> vals, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : vals) sum += v.get();
            out.set(sum);
            ctx.write(key, out);
        }
    }

    // Job builder 
    public static Job buildJob(Configuration conf, Path input, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step1-ExtractPredicates");
        job.setJarByClass(Step1_ExtractPredicates.class);

        job.setMapperClass(ExtractMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
