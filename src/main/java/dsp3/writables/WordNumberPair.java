package dsp3.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordNumberPair implements WritableComparable<WordNumberPair> {
    private Text word;
    private LongWritable number;

    public WordNumberPair() {
        this.word = new Text();
        this.number = new LongWritable();
    }

    public WordNumberPair(Text word, LongWritable number) {
        this.word = word;
        this.number = number;
    }

    public Text getWord() {
        return word;
    }

    public LongWritable getNumber() {
        return number;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        number.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        number.readFields(in);
    }

    public void set(Text word, LongWritable number) {
        this.word = word;
        this.number = number;
    }

    @Override
    public int compareTo(WordNumberPair other) {
        int cmp = this.word.compareTo(other.word);
        if (cmp != 0) {
            return cmp;
        }
        return this.number.compareTo(other.number);
    }
    
}
