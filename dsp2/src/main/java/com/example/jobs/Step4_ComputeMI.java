package com.example.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.*;

/**
 * Step 4:
 * Inputs:
 *  A) Step3 output:
 *     pred \t slot \t word \t cpsw \t cps
 *     where cpsw = C(p,slot,w), cps = C(p,slot)
 *
 *  B) Step2 totals:
 *     "SW\t slot \t word" \t csw   where csw = C(slot,w)
 *     "SLOT\t slot" \t cslot       where cslot = C(slot)
 *
 * Output:
 *   pred \t slot \t word \t MI
 */
public class Step4_ComputeMI {

    public static class FromJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step3 line: pred \t slot \t word \t cpsw \t cps
            String[] f = value.toString().split("\t");
            if (f.length != 5) return;

            String pred = f[0];
            String slot = f[1];
            String word = f[2];
            String cpsw = f[3];
            String cps = f[4];

            // join key: slot \t word
            outKey.set(slot + "\t" + word);
            // record: P \t pred \t cpsw \t cps
            outVal.set("P\t" + pred + "\t" + cpsw + "\t" + cps);
            ctx.write(outKey, outVal);
        }
    }

    public static class SWTotalsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step2 line: key \t value
            String[] kv = value.toString().split("\t");
            if (kv.length != 2) return;

            String k = kv[0];
            String v = kv[1];

            if (!k.startsWith("SW\t")) return;

            String[] p = k.split("\t", 3);
            if (p.length != 3) return;

            String slot = p[1];
            String word = p[2];

            outKey.set(slot + "\t" + word);
            outVal.set("S\t" + v); // csw
            ctx.write(outKey, outVal);
        }
    }

    public static class MIReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private long cSlotX = 1;
        private long cSlotY = 1;

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            String totalsDir = conf.get("dirt.totals.dir");
            if (totalsDir == null) return;

            Path totalsPath = new Path(totalsDir);
            FileSystem fs = totalsPath.getFileSystem(conf);
            if (!fs.exists(totalsPath)) return;

            for (FileStatus st : fs.listStatus(totalsPath)) {
                if (!st.isFile()) continue;
                String name = st.getPath().getName();
                if (!name.startsWith("part-")) continue;

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(st.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] kv = line.split("\t");
                        if (kv.length != 2) continue;

                        // kv[0]  "SLOT\tX" أو "SLOT\tY"
                        if (kv[0].equals("SLOT\tX")) {
                            cSlotX = Long.parseLong(kv[1]);
                        } else if (kv[0].equals("SLOT\tY")) {
                            cSlotY = Long.parseLong(kv[1]);
                        }
                    }
                }
            }

            // safety
            if (cSlotX <= 0) cSlotX = 1;
            if (cSlotY <= 0) cSlotY = 1;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            Long csw = null; // C(slot,w)
            List<String[]> preds = new ArrayList<>(); // (pred, cpsw, cps)

            for (Text t : vals) {
                String[] p = t.toString().split("\t");
                if (p.length < 2) continue;

                if (p[0].equals("S")) {
                    csw = Long.parseLong(p[1]);
                } else if (p[0].equals("P") && p.length == 4) {
                    preds.add(new String[]{p[1], p[2], p[3]});
                }
            }

            if (csw == null || csw <= 0) return;

            String[] sw = key.toString().split("\t", 2);
            if (sw.length != 2) return;
            String slot = sw[0];

            long cslot = slot.equals("X") ? cSlotX : cSlotY;
            if (cslot <= 0) return;

            for (String[] r : preds) {
                String pred = r[0];
                long cpsw = Long.parseLong(r[1]); // C(p,slot,w)
                long cps = Long.parseLong(r[2]);  // C(p,slot)

                if (cpsw <= 0 || cps <= 0) continue;

                double mi = Math.log(((double) cpsw * (double) cslot) / ((double) cps * (double) csw));

                ctx.write(new Text(pred + "\t" + slot + "\t" + sw[1]), new DoubleWritable(mi));
            }
        }
    }

    public static Job buildJob(Configuration conf, Path step3Join, Path step2Totals, Path output, int reducers) throws Exception {
        Job job = Job.getInstance(conf, "Step4-ComputeMI");
        job.setJarByClass(Step4_ComputeMI.class);

        job.getConfiguration().set("dirt.totals.dir", step2Totals.toString());

        job.setReducerClass(MIReducer.class);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, step3Join, TextInputFormat.class, FromJoinMapper.class);
        MultipleInputs.addInputPath(job, step2Totals, TextInputFormat.class, SWTotalsMapper.class);

        TextOutputFormat.setOutputPath(job, output);
        return job;
    }
}
