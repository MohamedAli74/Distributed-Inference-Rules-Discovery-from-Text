package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.example.helpers.Parser;
import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;
import com.example.helpers.Token;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;

public class Step1_ExtractPredicates {
    /**
     * Input: biarc line - head_word \t syntactic-ngram \t total_count \t counts_by_year
     * Output: key = predicate \t slot \t word , value = count
     * [in MI terms: |p,Slot,w|->count]
     */

   
    private static final Pattern CLEAN_WORD =
            Pattern.compile("^[A-Za-z](?:[A-Za-z]|['-](?=[A-Za-z])){1,}$"); // length >=2

    
    private static String cleanTemplate(String t) {
        if (t == null) return "";
        String s = t.replaceAll("[^A-Za-zXY_ ]+", " ");
        s = s.replaceAll("\\s+", " ").trim();
        if (!s.matches(".*[A-Za-z]{2,}.*")) return "";
        return s;
    }

    private static String norm(String w) {
        if (w == null) return "";
        return w.trim();
    }

    private static boolean isCleanWord(String w) {
        if (w == null) return false;
        String s = w.trim();
        if (s.isEmpty()) return false;
        return CLEAN_WORD.matcher(s).matches();
    }

    public static class ExtractMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();
        private final PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            Parser.ParsedLine pl = Parser.parseLine(value.toString());
            if (pl == null || pl.tokens == null || pl.tokens.isEmpty()) return;

            Token root = Parser.findRootVerb(pl.tokens);
            if (root == null) return;

            String rootWord = norm(root.word);
            if (!isCleanWord(rootWord)) return;

            // skip auxiliary verbs
            if (TestData.isAuxiliary(rootWord, stemmer)) return;

            Optional<Parser.PredicateInstance> instOpt = Parser.extractPredicate(pl.tokens, root, stemmer);
            if (!instOpt.isPresent()) return;

            Parser.PredicateInstance inst = instOpt.get();
            int c = pl.count;
            if (c <= 0) return;

            String template = cleanTemplate(inst.template);
            if (template.isEmpty()) return;

            String x = norm(inst.xWordStem);
            String y = norm(inst.yWordStem);

            boolean wrote = false;

            outVal.set(c);

            if (isCleanWord(x)) {
                outKey.set(template + "\tX\t" + x.toLowerCase());
                ctx.write(outKey, outVal);
                wrote = true;
            }

            if (isCleanWord(y)) {
                outKey.set(template + "\tY\t" + y.toLowerCase());
                ctx.write(outKey, outVal);
                wrote = true;
            }

            if (!wrote) return;
        }
    }

    /** Reducer: sum counts */
    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable out = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : vals) sum += v.get();
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
        job.setMapOutputValueClass(LongWritable.class);

        // Step1 output = SequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        TextInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
