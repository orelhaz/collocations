package models;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DecadeOccurrencesPair extends TaggedValue<Text, IntWritable> implements WritableComparable<DecadeOccurrencesPair>
{

	public DecadeOccurrencesPair() {
        super();
    }
 
    public DecadeOccurrencesPair(Text tag) {
        super(tag);
    }
 
    public DecadeOccurrencesPair(Text tag, IntWritable value) {
        super(tag,value);
    }
 
    @Override
    protected void init() {
        tag = new Text();
        value = new IntWritable();
    }

    @Override
    public int compareTo(DecadeOccurrencesPair other) {
      
    	// negative means BEFORE
    	int myDecade = Integer.parseInt(getTag().toString());
    	int myOccurrences = getvalue().get();
    	
    	int otherDecade = Integer.parseInt(other.getTag().toString());
    	int otherOccurrences = other.getvalue().get();
    	
    	// sort decades from the smallest to highest
    	int decadesApart = myDecade - otherDecade;
    	if (decadesApart != 0)
    		return decadesApart;
    	
    	//same decade, more occurrences should come first
    	return (int)(otherOccurrences - myOccurrences);
    }
	

}
