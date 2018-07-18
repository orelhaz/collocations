import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Orel on 18/07/2018.
 */
class DecadeCount implements WritableComparable<DecadeCount> {

    Text decade;
    IntWritable count;

    public DecadeCount() {
        set(new Text(), new IntWritable());
    }

    DecadeCount(String decade, IntWritable count) {
        set(new Text(decade), count);
    }

    public Text getDecade() {
        return decade;
    }

    public IntWritable getCount() {
        return count;
    }

    public void set(Text decade, IntWritable count) {
        this.decade = decade;
        this.count= count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);
        count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        count.write(out);
    }

    @Override
    public int hashCode() {
        return decade.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return decade.toString() + '\t' + count.toString();
    }

    @Override
    public int compareTo(DecadeCount other) {
        int result = decade.compareTo(other.decade);
        if (result != 0) return result;
        return count.compareTo(other.count);
    }
}