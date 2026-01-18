package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

/**
 * Step 4:
 * Inputs:
 *  A) Step3 output (TextOutputFormat):
 *     pred \t slot \t word \t cpsw \t cps
 *     where cpsw = C(p,slot,w), cps = C(p,slot)
 *
 *  B) Step2 totals (TextOutputFormat):
 *     key="SW\t slot \t word"   value=csw    -> line: SW \t slot \t word \t csw
 *     key="SLOT\t slot"        value=cslot  -> line: SLOT \t slot \t cslot
 *
 * Output:
 *   key:   pred \t slot \t word
 *   value: MI
 */
public class Step4_ComputeMI {

    /** Mapper from Step3 join output */
    public static class FromJoinMapper extends Mapper<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(Text key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step3 line: pred \t slot \t word \t cpsw \t cps  (5 fields)
            String[] k = key.toString().split("\t");
            String[] v = value.toString().split("\t");
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
            // key:SW\t<slot>\t<word>       value:csw
            //or:PS\t<predicate>\t<slot>    value:cps
            //or:SLOT\t<slot>               value:cslot
            // we only want SW records
            // Example: "SW\tX\tdrug\t15"
            if (!key.toString().startsWith("SW\t")) return;

            // key = "SW\t<slot>\t<word>"
            String[] p = key.toString().split("\t", -1);
            if (p.length != 3) return;

            String slot = p[1];
            String word = p[2];

            outKey.set(slot + "\t" + word);
            outVal.set("S\t" + value.get()); // csw
            ctx.write(outKey, outVal);
        }
    }

    public static class MIReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private long cSlotX = 1;
        private long cSlotY = 1;

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            Path totalsPath = new Path(conf.get("dirt.totals.dir"));
            FileSystem fs = totalsPath.getFileSystem(conf);

            // Iterate over all part-files in the folder
            for (FileStatus st : fs.listStatus(totalsPath)) {
                if (st.getPath().getName().startsWith("_")) continue; // Skip _SUCCESS

                // USE SEQUENCE FILE READER
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()));
                
                // Create objects to hold the data
                Text key = new Text();
                Text val = new Text(); // Or LongWritable, depending on Step 2 output

                while (reader.next(key, val)) {
                    String keyStr = key.toString();
                    long count = Long.parseLong(val.toString());

                    if (keyStr.equals("SLOT\tX")) {
                        cSlotX = count;
                    } else if (keyStr.equals("SLOT\tY")) {
                        cSlotY = count;
                    }
                }
                reader.close();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            Long csw = null;                 // C(slot, w)
            List<String[]> preds = new ArrayList<>(); // (pred, cpsw, cps)

            for (Text t : vals) {
                String[] p = t.toString().split("\t");
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

                ctx.write(new Text(pred + "\t" + slot + "\t" + word), new DoubleWritable(mi));
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

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, step3Join, SequenceFileInputFormat.class, FromJoinMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, SequenceFileInputFormat.class, SWTotalsMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
