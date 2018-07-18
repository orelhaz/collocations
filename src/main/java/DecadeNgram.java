import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Orel on 18/07/2018.
 */
class DecadeNgram implements WritableComparable<DecadeNgram> {

    Text decade;
    Text ngram;

    public DecadeNgram() {
        set(new Text(), new Text());
    }

    DecadeNgram(String decade, String ngram) {
        set(new Text(decade), new Text(ngram));
    }

    public Text getDecade() {
        return decade;
    }


    public Text getNgram() {
        return ngram;
    }

    public void set(Text decade, Text ngram) {
        this.decade = decade;
        this.ngram = ngram;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);
        ngram.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        ngram.write(out);
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
        return decade.toString() + '\t' + ngram.toString();
    }

    @Override
    public int compareTo(DecadeNgram other) {
        int result = decade.compareTo(other.decade);
        if (result != 0) return result;
        return ngram.compareTo(other.ngram);
    }
}