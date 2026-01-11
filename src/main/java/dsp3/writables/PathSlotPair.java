package dsp3.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PathSlotPair implements WritableComparable<PathSlotPair> {
    private Text path;
    private Text slot;

    public PathSlotPair() {
        this.path = new Text();
        this.slot = new Text();
    }
    public PathSlotPair(Text path, Text slot) {
        this.path = path;
        this.slot = slot;
    }
    public Text getPath() {
        return path;
    }
    public Text getSlot() {
        return slot;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        path.write(out);
        slot.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        path.readFields(in);
        slot.readFields(in);
    }

    public void set(Text path, Text slot) {
        this.path = path;
        this.slot = slot;
    }

    @Override
    public int compareTo(PathSlotPair other) {
        int cmp = this.path.compareTo(other.path);
        if (cmp != 0) {
            return cmp;
        }
        return this.slot.compareTo(other.slot);
    }
}
