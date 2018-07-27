package models;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DecadeText extends TaggedValue<Text, Text> implements WritableComparable<DecadeText> {

	public DecadeText() {}
	
	public DecadeText (Text tag, Text value)
	{
		super (tag, value);
	}
	
	public DecadeText (String tag, String value)
	{
		super (new Text(tag), new Text(value));
	}
	
	@Override
	public int compareTo(DecadeText other) {
		int result = getTag().compareTo(other.getTag());
        if (result != 0) 
        	return result;
        return getvalue().compareTo(other.getvalue());
	}

	@Override
	protected void init() {
		tag = new Text();
        value = new Text();
		
	}
	
	public int hashCode() {
        return getTag().hashCode();
    }
	
	@Override
    public String toString() {
        return getTag().toString() + '\t' + getvalue().toString();
    }

}
