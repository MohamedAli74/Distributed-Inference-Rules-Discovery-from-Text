package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.example.helpers.Parser;
import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;
import com.example.helpers.Token;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Step1 (Merged): Extract predicates + compute global totals in the SAME MR job.
 *
 * Input (TEXT): biarc line
 *   head_word \t syntactic-ngram \t total_count \t counts_by_year
 *
 * Output (SequenceFile: Text -> LongWritable):
 *   PSW \t pred \t slot \t word   -> C(p,slot,w)
 *   PS  \t pred \t slot           -> C(p,slot)
 *   SW  \t slot \t word           -> C(slot,w)
 *   SLOT\t slot                   -> C(slot)
 */
public class Step1_ExtractAndTotals {

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

    public static class MergedMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();
        private final PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            Parser.ParsedLine pl;
            try {
                pl = Parser.parseLine(line);
            } catch (Exception e) {
                ctx.getCounter("Step1", "MALFORMED_LINE").increment(1);
                return;
            }

            if (pl == null || pl.tokens == null || pl.tokens.isEmpty()) return;

            Token root = Parser.findRootVerb(pl.tokens);
            if (root == null) return;

            String rootWord = norm(root.word);
            if (!isCleanWord(rootWord)) return;

            // skip auxiliary verbs
            if (TestData.isAuxiliary(rootWord.toLowerCase(), stemmer)) return;

            Optional<Parser.PredicateInstance> instOpt = Parser.extractPredicate(pl.tokens, root, stemmer);
            if (!instOpt.isPresent()) return;

            Parser.PredicateInstance inst = instOpt.get();

            long c = pl.count;
            if (c <= 0) return;

            String pred = cleanTemplate(inst.template);
            if (pred.isEmpty()) return;

            String x = norm(inst.xWordStem);
            String y = norm(inst.yWordStem);

            outVal.set(c);

            if (isCleanWord(x)) emitAll(ctx, pred, "X", x.toLowerCase(), c);
            if (isCleanWord(y)) emitAll(ctx, pred, "Y", y.toLowerCase(), c);
        }

        private void emitAll(Context ctx, String pred, String slot, String word, long c) throws IOException, InterruptedException {
            // PSW
            outKey.set("PSW\t" + pred + "\t" + slot + "\t" + word);
            ctx.write(outKey, outVal);

            // PS
            outKey.set("PS\t" + pred + "\t" + slot);
            ctx.write(outKey, outVal);

            // SW
            outKey.set("SW\t" + slot + "\t" + word);
            ctx.write(outKey, outVal);

            // SLOT
            outKey.set("SLOT\t" + slot);
            ctx.write(outKey, outVal);
        }
    }

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

    public static Job buildJob(Configuration conf, Path input, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step1-ExtractAndTotals");
        job.setJarByClass(Step1_ExtractAndTotals.class);

        job.setMapperClass(MergedMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(reducers);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
