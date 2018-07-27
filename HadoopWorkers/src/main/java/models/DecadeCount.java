package models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DecadeCount extends TaggedValue<Text, DoubleWritable> implements WritableComparable<DecadeCount> {

	
	public DecadeCount() {}
	
	public DecadeCount (Text tag, DoubleWritable value)
	{
		super (tag, value);
	}
	
	public DecadeCount (String tag, DoubleWritable value)
	{
		super (new Text(tag), value);
	}
	
	public Text getDecade()
	{
		return getTag();
	}
	
	public DoubleWritable getCount()
	{
		return getvalue();
	}
	
	@Override
	public int compareTo(DecadeCount other) {
		int result = getTag().compareTo(other.getTag());
        if (result != 0) 
        	return result;
        return -1 * getvalue().compareTo(other.getvalue());
	}

	@Override
	protected void init() {
		tag = new Text();
        value = new DoubleWritable();
		
	}
	
	public int hashCode() {
        return getTag().hashCode();
    }
	
	@Override
    public String toString() {
        return getTag().toString() + '\t' + getvalue().toString();
    }

}
