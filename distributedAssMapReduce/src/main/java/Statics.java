import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class Statics {

	public final static String VERSION = "001";
	public final static String DEFAULT_REGION = "us-east-1";
	public final static String DEFAULT_PLACEMENT_REGION = "us-east-1a";
	public final static String BUCKET_URL = "s3n://orelhaz.distributed.ass12/";
	//public static String BUCKET_URL = "s3n://dsps-ori-refael/ass2/";
	//public static String DEFAULT_SECURITY_GROUP = "sg-49e94400";
	public final static String DEFAULT_SECURITY_GROUP = "sg-ff8865b6";

	//public final static String KEY_PAIR = "aws-key-pair";
	public final static String KEY_PAIR = "dsdp";

	public static String WorkerJarName = "hadoopWorker.jar";
	public static String HebBigrmas = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
	public static String EnBigrmas = "s3n://orelhaz.distributed.ass12/googlebooks-eng-all-2gram-20120701-xz";
	//public static String EnBigrmas = "s3n://dsps-ori-refael/ass2/input/googlebooks-eng-all-2gram-20120701-xz";
	//public static String EnBigrmas = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
	
	public static String SplitWordsOutput = BUCKET_URL + "split-words-output";
	public static String ExtractCountOutput = BUCKET_URL + "extract-counts-output";
	public static String ExtractRatioOutput = BUCKET_URL + "extract-ratio-output";
	public static String TopCollocationsOutput = BUCKET_URL + "top-collocations-output";
	
	
	public static String TopCollocation = "100";
	
	public enum RunConfiguration {All, WordCount, Collocation}
	public enum InputLang { Hebrew, English };
	
	public static AWSStaticCredentialsProvider getCredentials()
	{
		return new AWSStaticCredentialsProvider(new DefaultAWSCredentialsProviderChain().getCredentials());
	}
}
