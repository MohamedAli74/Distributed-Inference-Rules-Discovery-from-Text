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

    /** ===== helper: parse a TextOutputFormat line into (key,value) using last TAB ===== */
    private static class KV {//TODO
        String key;
        String val;
    }

    private static KV parseLineKV(String line) {//TODO
        if (line == null) return null;
        line = line.trim();
        if (line.isEmpty()) return null;

        int lastTab = line.lastIndexOf('\t');
        if (lastTab < 0) return null;

        KV kv = new KV();
        kv.key = line.substring(0, lastTab);
        kv.val = line.substring(lastTab + 1);
        return kv;
    }

    /** Mapper from Step3 join output */
    public static class FromJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step3 line: pred \t slot \t word \t cpsw \t cps  (5 fields)
            String[] f = value.toString().split("\t");
            if (f.length != 5) return;

            String pred = f[0];
            String slot = f[1];
            String word = f[2];
            String cpsw = f[3];//TODO
            String cps  = f[4];//TODO

            // join key: slot \t word
            outKey.set(slot + "\t" + word);

            // record: P \t pred \t cpsw \t cps
            outVal.set("P\t" + pred + "\t" + cpsw + "\t" + cps);
            ctx.write(outKey, outVal);
        }
    }

    /** Mapper from Step2 totals: only SW records */
    public static class SWTotalsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Step2 totals line is TextOutputFormat: "<key>\t<value>"
            // Example: "SW\tX\tdrug\t15"
            KV kv = parseLineKV(value.toString());
            if (kv == null) return;

            if (!kv.key.startsWith("SW\t")) return;

            // kv.key = "SW\t<slot>\t<word>"
            String[] p = kv.key.split("\t", 3);
            if (p.length != 3) return;

            String slot = p[1];
            String word = p[2];

            outKey.set(slot + "\t" + word);
            outVal.set("S\t" + kv.val); // csw
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
                        KV kv = parseLineKV(line);
                        if (kv == null) continue;

                        // SLOT totals are stored as key="SLOT\tX" value="<count>"
                        if (kv.key.equals("SLOT\tX")) {
                            try { cSlotX = Long.parseLong(kv.val); } catch (Exception ignored) {}
                        } else if (kv.key.equals("SLOT\tY")) {
                            try { cSlotY = Long.parseLong(kv.val); } catch (Exception ignored) {}
                        }
                    }
                }
            }

            if (cSlotX <= 0) cSlotX = 1;
            if (cSlotY <= 0) cSlotY = 1;
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
