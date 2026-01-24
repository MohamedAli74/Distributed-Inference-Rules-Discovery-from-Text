package com.example.jobs;

import com.example.helpers.PorterStemmer;
import com.example.helpers.TestData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Step2: (Sequence -> Text)
 *
 * Input (SequenceFile) from Step1_ExtractAndTotals:
 *   PSW \t pred \t slot \t word   -> cpsw
 *   PS  \t pred \t slot           -> cps
 *   SW  \t slot \t word           -> csw
 *   SLOT\t slot                   -> cslot
 *
 * Output (TEXT, one line per record):
 *   pred \t slot \t word \t mi
 */
public class Step2_ComputeMI {

    public static class MergedMapper extends Mapper<Text, LongWritable, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        private final PorterStemmer stemmer = new PorterStemmer();
        private Set<String> testPreds;

        // PS totals for ONLY test predicates: key = pred\tslot -> cps
        private final Map<String, Long> psTotals = new HashMap<>();

        private static String psKey(String pred, String slot) {
            return pred + "\t" + slot;
        }

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] files = ctx.getCacheFiles();
            testPreds = TestData.loadTestPredicates(files, stemmer);

            Configuration conf = ctx.getConfiguration();
            Path totalsDir = new Path(conf.get("dirt.step1.dir"));
            loadPSTotals(ctx, conf, totalsDir);
        }

        private void loadPSTotals(Context ctx, Configuration conf, Path dir) throws IOException {
            FileSystem fs = dir.getFileSystem(conf);
            if (!fs.exists(dir)) return;

            FileStatus[] statuses = fs.listStatus(dir);
            if (statuses == null) return;

            Text k = new Text();
            LongWritable v = new LongWritable();

            for (FileStatus st : statuses) {
                String name = st.getPath().getName();
                if (name.startsWith("_") || name.startsWith(".")) continue;

                try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()))) {
                    while (reader.next(k, v)) {
                        String ks = k.toString();
                        if (!ks.startsWith("PS\t")) continue;

                        // "PS\tpred\tslot"
                        String[] p = ks.split("\t", -1);
                        if (p.length != 3) continue;

                        String pred = p[1].trim();
                        String slot = p[2].trim();
                        if (pred.isEmpty() || slot.isEmpty()) continue;

                        if (testPreds != null && !testPreds.contains(pred)) continue;

                        psTotals.put(psKey(pred, slot), v.get());
                    }
                } catch (Exception e) {
                    ctx.getCounter("Step2", "PS_TOTALS_READ_ERROR").increment(1);
                }
            }
        }

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            String ks = key.toString();
            if (ks == null || ks.isEmpty()) return;

            // 1) SW: "SW\t<slot>\t<word>" -> csw
            if (ks.startsWith("SW\t")) {
                String[] p = ks.split("\t", -1);
                if (p.length != 3) return;

                String slot = p[1].trim();
                String word = p[2].trim();
                if (slot.isEmpty() || word.isEmpty()) return;

                outKey.set(slot + "\t" + word);
                outVal.set("S\t" + value.get());
                ctx.write(outKey, outVal);
                return;
            }

            // 2) PSW: "PSW\t<pred>\t<slot>\t<word>" -> cpsw
            if (ks.startsWith("PSW\t")) {
                String[] p = ks.split("\t", -1);
                if (p.length != 4) return;

                String pred = p[1].trim();
                String slot = p[2].trim();
                String word = p[3].trim();

                if (pred.isEmpty() || slot.isEmpty() || word.isEmpty()) return;

                if (testPreds != null && !testPreds.contains(pred)) return;

                Long cps = psTotals.get(psKey(pred, slot));
                if (cps == null || cps <= 0) return;

                long cpsw = value.get();
                if (cpsw <= 0) return;

                outKey.set(slot + "\t" + word);
                outVal.set("P\t" + pred + "\t" + cpsw + "\t" + cps);
                ctx.write(outKey, outVal);
            }
        }
    }

    public static class MIReducer extends Reducer<Text, Text, Text, NullWritable> {

        private long cSlotX = 1;
        private long cSlotY = 1;

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            Path totalsDir = new Path(conf.get("dirt.step1.dir"));
            FileSystem fs = totalsDir.getFileSystem(conf);

            FileStatus[] statuses = fs.listStatus(totalsDir);
            if (statuses == null) return;

            for (FileStatus st : statuses) {
                if (st.getPath().getName().startsWith("_")) continue;

                try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()))) {
                    Text key = new Text();
                    LongWritable val = new LongWritable();

                    while (reader.next(key, val)) {
                        String ks = key.toString();
                        long count = val.get();

                        if ("SLOT\tX".equals(ks)) cSlotX = count;
                        else if ("SLOT\tY".equals(ks)) cSlotY = count;
                    }
                } catch (Exception e) {
                    ctx.getCounter("Step2", "SLOT_TOTALS_READ_ERROR").increment(1);
                }
            }
        }

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
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            Long csw = null; // C(slot,w)
            List<String[]> preds = new ArrayList<>(); // (pred, cpsw, cps)

            for (Text t : vals) {
                String[] p = t.toString().split("\t", -1);
                if (p.length < 2) continue;

                if ("S".equals(p[0])) {
                    try { csw = Long.parseLong(lastNonEmpty(p)); } catch (Exception ignored) {}
                } else if ("P".equals(p[0]) && p.length == 4) {
                    preds.add(new String[]{p[1], p[2], p[3]});
                }
            }

            if (csw == null || csw <= 0) return;

            // key = "<slot>\t<word>"
            String[] sw = key.toString().split("\t", 2);
            if (sw.length != 2) return;

            String slot = sw[0];
            String word = sw[1];

            long cslot;
            if ("X".equals(slot)) cslot = cSlotX;
            else if ("Y".equals(slot)) cslot = cSlotY;
            else return;

            if (cslot <= 0) return;

            for (String[] r : preds) {
                String pred = r[0];

                long cpsw, cps;
                try { cpsw = Long.parseLong(r[1]); } catch (Exception e) { continue; }
                try { cps  = Long.parseLong(r[2]); } catch (Exception e) { continue; }

                if (cpsw <= 0 || cps <= 0) continue;

                // MI = log( (C(p,slot,w) * C(slot)) / (C(p,slot) * C(slot,w)) )
                double mi = Math.log(((double) cpsw * (double) cslot) / ((double) cps * (double) csw));

                String line = pred + "\t" + slot + "\t" + word + "\t" + String.format(Locale.US, "%.6f", mi);
                ctx.write(new Text(line), NullWritable.get());
            }
        }
    }

    public static Job buildJob(Configuration conf,
                               Path step1TotalsDir,
                               Path output,
                               Path positive,
                               Path negative,
                               int reducers) throws Exception {

        Job job = Job.getInstance(conf, "Step2-ComputeMI");
        job.setJarByClass(Step2_ComputeMI.class);

        // for mapper/reducer setup() to scan Step1 totals
        job.getConfiguration().set("dirt.step1.dir", step1TotalsDir.toString());

        // IMPORTANT: avoid extra tab at end of line
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        // Input = SequenceFile (Text, LongWritable)
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, step1TotalsDir);

        job.setMapperClass(MergedMapper.class);
        job.setReducerClass(MIReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Output = Text
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, output);

        // cache test files (to load test predicates)
        job.getConfiguration().setBoolean("mapreduce.job.cache.symlink.create", true);
        job.addCacheFile(new URI(positive.toString() + "#positive.txt"));
        job.addCacheFile(new URI(negative.toString() + "#negative.txt"));

        return job;
    }
}
