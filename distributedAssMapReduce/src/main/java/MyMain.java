import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class MyMain {

	private static boolean _debug = true;
	private static void debug (String s) {
		if (_debug)
			System.out.println(s);
	}

	public static void main(String[] args) {
		Statics.RunConfiguration runConf = Statics.RunConfiguration.Collocation; //default value
		Statics.InputLang lang = Statics.InputLang.Hebrew; //default value

		// Set running project
		if (args.length > 0)
		{
				switch (args[0])
			{
				case "ExtractCollations":
					runConf = Statics.RunConfiguration.Collocation;
					break;

				default:
					break;
			}
		}

		// Set input language
		if (args.length > 1)
		{
			switch (args[1])
			{
				case "en":
					lang = Statics.InputLang.English;
					break;
				case "heb":
					System.out.println("cascdsa");
					lang = Statics.InputLang.Hebrew;
					break;

			}
		}

		// input by language
		String input = lang == Statics.InputLang.English ? Statics.EnBigrmas : Statics.HebBigrmas;

		debug("------------------------------");
		debug("Printing Constants..");
		debug("Region: " + Statics.DEFAULT_REGION);
		debug("S3 bucket: " + Statics.BUCKET_URL);
		debug("Key-Pair: " + Statics.KEY_PAIR);
		debug("Machines amount: " + Statics.MACHINES_AMOUNT);
		debug("Configurations: " + runConf);
		debug("Input url: " + input);
		debug("------------------------------");

		// Initializing the AWS map-reduce
		debug("Initializing...");
		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(Statics.getCredentials())
				.withRegion(Statics.DEFAULT_REGION)
				.build();


		debug("map reduce connection created...");

		debug ("creating steps...");
		// Gets pre-defined steps
		Collection<StepConfig> steps = GetSteps(runConf, input, lang);
		debug("steps created successfully...");

		// Setting up the configuration of the entire run
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
			    .withInstanceCount(Statics.MACHINES_AMOUNT)
			    .withMasterInstanceType(Statics.INSTANCE_TYPE)
			    .withSlaveInstanceType(Statics.INSTANCE_TYPE)
			    .withEc2KeyName(Statics.KEY_PAIR).withHadoopVersion("2.6.0")
			    .withAdditionalMasterSecurityGroups(Statics.DEFAULT_SECURITY_GROUP)
			    .withKeepJobFlowAliveWhenNoSteps(false)
			    .withPlacement(new PlacementType(Statics.DEFAULT_PLACEMENT_REGION));


		debug("Jobflow created...");

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			    .withName("DistributedAssignment2JobV"+ Statics.VERSION)
			    .withInstances(instances)
			    .withSteps(steps)
			    .withServiceRole("EMR_DefaultRole")
			    .withReleaseLabel("emr-5.11.0")
			    .withJobFlowRole("EMR_EC2_DefaultRole")
			    .withLogUri(Statics.BUCKET_URL + "logs/");

		debug("created flow request...");

		debug("trying to run job flow...");
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		debug("job flow running...");

		String jobFlowId = runJobFlowResult.getJobFlowId();
		debug ("ran job flow with id: " + jobFlowId);
    }

	/**
	 * Creates the necessary steps to run the assignment
	 * @return
	 */
	private static Collection<StepConfig> GetSteps(Statics.RunConfiguration configuration, String input, Statics.InputLang lang)
	{
		String langStr = lang == Statics.InputLang.English ? "en" : "heb";
		return GenerateAllJob(input, langStr);

	}

	private static List<StepConfig> GenerateAllJob (String input, String lang)
	{
		List<StepConfig> steps = new ArrayList<StepConfig>();
		String collocationJarName = Statics.BUCKET_URL + Statics.WorkerJarName;

		HadoopJarStepConfig hadoopJarFirstStep = new HadoopJarStepConfig()
			    .withJar(collocationJarName)
			    .withMainClass("workers.SplitWords")
			    .withArgs(input, Statics.SplitWordsOutput, lang);

		StepConfig step1Config = new StepConfig()
		    .withName("splitWords")
		    .withHadoopJarStep(hadoopJarFirstStep)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");

        steps.add(step1Config);

        HadoopJarStepConfig hadoopJarSecondStep = new HadoopJarStepConfig()
				.withJar(collocationJarName)
				.withMainClass("workers.ExtractCounts")
				.withArgs(Statics.SplitWordsOutput, Statics.ExtractCountOutput);

		StepConfig step2Config = new StepConfig()
				.withName("ExtractCounts")
				.withHadoopJarStep(hadoopJarSecondStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		steps.add(step2Config);

		HadoopJarStepConfig hadoopJarThirdStep = new HadoopJarStepConfig()
				.withJar(collocationJarName)
				.withMainClass("workers.ExtractLogRatio")
				.withArgs(Statics.ExtractCountOutput, Statics.ExtractRatioOutput);

		StepConfig step3Config = new StepConfig()
				.withName("ExtractLogRatio")
				.withHadoopJarStep(hadoopJarThirdStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		steps.add(step3Config);

		HadoopJarStepConfig hadoopJarFourthStep = new HadoopJarStepConfig()
				.withJar(collocationJarName)
				.withMainClass("workers.ExtractTopCollocations")
				.withArgs(Statics.ExtractRatioOutput, Statics.TopCollocationsOutput);

		StepConfig step4Config = new StepConfig()
				.withName("ExtractTopCollocations")
				.withHadoopJarStep(hadoopJarFourthStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		steps.add(step4Config);

		return steps;
	}
}
