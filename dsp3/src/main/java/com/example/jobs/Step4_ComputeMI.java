package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;   // ✅ TEXT output
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Step 4:
 * Inputs:
 *  A) Step3 output (SequenceFile):
 *     key:   pred \t slot \t word
 *     value: cpsw \t cps
 *
 *  B) Step2 totals (SequenceFile):
 *     key="SW\t slot \t word"   value=csw
 *     key="SLOT\t slot"        value=cslot
 *
 * Output (TEXT):
 *   line: pred \t slot \t word \t mi
 */
public class Step4_ComputeMI {

    /** Mapper from Step3 join output */
    public static class FromJoinMapper extends Mapper<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step3: key = pred \t slot \t word  (3 fields)
            //       val = cpsw \t cps           (2 fields)
            String[] k = key.toString().split("\t", -1);
            String[] v = value.toString().split("\t", -1);
            if (k.length != 3) return;
            if (v.length != 2) return;

            String pred = k[0];
            String slot = k[1];
            String word = k[2];
            String cpsw = v[0];
            String cps  = v[1];

            // join key: slot \t word
            outKey.set(slot + "\t" + word);

            // value: P \t pred \t cpsw \t cps
            outVal.set("P\t" + pred + "\t" + cpsw + "\t" + cps);
            ctx.write(outKey, outVal);
        }
    }

    /** Mapper from Step2 totals: only SW records */
    public static class SWTotalsMapper extends Mapper<Text, LongWritable, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
            // Step2 totals: key examples:
            // "SW\tX\tdrug"   value: csw
            // "SLOT\tX"       value: cslot
            // We only want SW records here
            if (!key.toString().startsWith("SW\t")) return;

            String[] p = key.toString().split("\t", -1);
            if (p.length != 3) return;

            String slot = p[1];
            String word = p[2];

            outKey.set(slot + "\t" + word);
            outVal.set("S\t" + value.get()); // csw
            ctx.write(outKey, outVal);
        }
    }

    /**
     * Reducer:
     * key = slot \t word
     * values include:
     *  - S \t csw
     *  - P \t pred \t cpsw \t cps
     *
     * Output TEXT line:
     *   pred \t slot \t word \t mi
     */
    public static class MIReducer extends Reducer<Text, Text, Text, Text> {

        private long cSlotX = 1;
        private long cSlotY = 1;

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            Path totalsPath = new Path(conf.get("dirt.totals.dir"));
            FileSystem fs = totalsPath.getFileSystem(conf);

            // Iterate over all part-files in the folder
            for (FileStatus st : fs.listStatus(totalsPath)) {
                if (st.getPath().getName().startsWith("_")) continue; // skip _SUCCESS etc.

                // Step2 totals are SequenceFiles in your pipeline
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()));

                Text key = new Text();
                LongWritable val = new LongWritable();

                while (reader.next(key, val)) {
                    String keyStr = key.toString();
                    long count = val.get();

                    if (keyStr.equals("SLOT\tX")) cSlotX = count;
                    else if (keyStr.equals("SLOT\tY")) cSlotY = count;
                }

                reader.close();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            Long csw = null; // C(slot,w)
            List<String[]> preds = new ArrayList<>(); // (pred, cpsw, cps)

            for (Text t : vals) {
                String[] p = t.toString().split("\t", -1);
                if (p.length < 2) continue;

                if (p[0].equals("S")) {
                    try { csw = Long.parseLong(p[1]); } catch (Exception ignored) {}
                } else if (p[0].equals("P") && p.length == 4) {
                    preds.add(new String[]{p[1], p[2], p[3]});
                }
            }

            if (csw == null || csw <= 0) return;

            // key = "<slot>\t<word>"
            String[] sw = key.toString().split("\t", 2);
            if (sw.length != 2) return;

            String slot = sw[0];
            String word = sw[1];

            long cslot = slot.equals("X") ? cSlotX : cSlotY;
            if (cslot <= 0) return;

            for (String[] r : preds) {
                String pred = r[0];

                long cpsw, cps;
                try { cpsw = Long.parseLong(r[1]); } catch (Exception e) { continue; }
                try { cps  = Long.parseLong(r[2]); } catch (Exception e) { continue; }

                if (cpsw <= 0 || cps <= 0) continue;

                // MI = log( (C(p,slot,w) * C(slot)) / (C(p,slot) * C(slot,w)) )
                double mi = Math.log(((double) cpsw * (double) cslot) / ((double) cps * (double) csw));

                // ✅ Output as TEXT: "pred\tslot\tword\t<mi>"
                String line = pred + "\t" + slot + "\t" + word + "\t" +
                        String.format(Locale.US, "%.6f", mi);

                ctx.write(new Text(line), new Text("")); // value empty -> still writes trailing tab sometimes depending on settings
            }
        }
    }

    public static Job buildJob(Configuration conf, Path step3Join, Path step2Totals, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step4-ComputeMI");
        job.setJarByClass(Step4_ComputeMI.class);

        // for reducer setup() to read SLOT totals
        job.getConfiguration().set("dirt.totals.dir", step2Totals.toString());

        job.setReducerClass(MIReducer.class);
        job.setNumReduceTasks(reducers);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer outputs TEXT
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Inputs stay SequenceFile (like your current code)
        MultipleInputs.addInputPath(job, step3Join, SequenceFileInputFormat.class, FromJoinMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, SequenceFileInputFormat.class, SWTotalsMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
