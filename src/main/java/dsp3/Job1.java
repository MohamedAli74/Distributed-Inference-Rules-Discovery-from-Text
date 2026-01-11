package dsp3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import dsp3.writables.PathSlotPair;
import dsp3.writables.WordNumberPair;
import org.apache.hadoop.io.LongWritable;
import dsp3.utils;

public class Job1 {
    //input extraction and path aggregation job
    public static class job1Mapper extends Mapper<LongWritable, Text, PathSlotPair, WordNumberPair> {
        private PathSlotPair outKey = new PathSlotPair();
        private WordNumberPair outValue = new WordNumberPair();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /* 
             input: head_word <TAB> syntactic-ngram                 <TAB> total_count <TAB> counts_by_year
                                       -->(Word/POS/Relation/Parent-ID )*         
             */
             //example: cease   cease/VB/ccomp/0 for/IN/prep/1 an/DT/det/4 instant/NN/pobj/2    56      ...
            utils.parsedLine parsed = utils.parse(value.toString());
            if(!parsed.getSyntacticNgram()[0].getPos().equals("VB")){
                return; //only process verb-headed ngrams
            }
            String headWord = parsed.getHeadWord();
            utils.word[] syntacticNgram = parsed.getSyntacticNgram();
            int totalCount = parsed.getTotalCount();
            
        }
    }
}
