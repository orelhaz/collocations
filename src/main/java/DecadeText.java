import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Orel on 18/07/2018.
 */
class DecadeText implements WritableComparable<DecadeText> {

    Text decade;
    Text text;

    public DecadeText() {
        set(new Text(), new Text());
    }

    DecadeText(String decade, String word) {
        set(new Text(decade), new Text(word));
    }

    public Text getDecade() {
        return decade;
    }


    public Text getText() {
        return text;
    }

    public void set(Text decade, Text word) {
        this.decade = decade;
        this.text = word;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);
        text.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        text.write(out);
    }

    @Override
    public int hashCode() {
        return decade.hashCode();
    }

    @Override
    public String toString() {
        return decade.toString() + '\t' + text.toString();
    }

    @Override
    public int compareTo(DecadeText other) {
        int result = decade.compareTo(other.decade);
        if (result != 0) return result;
        return text.compareTo(other.text);
    }
}