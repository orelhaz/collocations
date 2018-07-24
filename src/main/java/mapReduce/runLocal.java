package mapReduce;

/**
 * Created by Orel on 21/07/2018.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import writeableClasses.DecadeCount;
import writeableClasses.DecadeCountComparator;
import writeableClasses.DecadeText;

public class runLocal {
    public static void main(String[] args) {

        // Example of applying a pipeline of two mapper/reducer jobs, by using JobControl object, over a depending constraint.

        try {

            String input = args[0];
            String output1 = args[1];
            String output2 = args[2];
            String output3 = args[3];
            String output = args[4];

            System.out.println(input);
            System.out.println(output1);
            System.out.println(output2);
            System.out.println(output3);
            System.out.println(output);

            Configuration conf = new Configuration();

            Job job1 = new Job(conf, "mapReduce.SplitWords");
            job1.setJarByClass(SplitWords.class);

            job1.setMapperClass(SplitWords.MapperClass.class);
            job1.setPartitionerClass(SplitWords.PartitionerClass.class);
            // job.setCombinerClass(SplitWords.ReducerClass.class);
            job1.setReducerClass(SplitWords.ReducerClass.class);
            job1.setMapOutputKeyClass(DecadeText.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);
            Path op1 = new Path(output1);
            op1.getFileSystem(conf).delete(op1, true);
            FileInputFormat.addInputPath(job1, new Path(input));
            FileOutputFormat.setOutputPath(job1, op1);


            Job job2 = new Job(conf, "mapReduce.ExtractCounts");
            job2.setJarByClass(ExtractCounts.class);

            job2.setMapperClass(ExtractCounts.MapperClass.class);
            job2.setPartitionerClass(ExtractCounts.PartitionerClass.class);
            // job.setCombinerClass(ExtractCounts.ReducerClass.class);
            job2.setReducerClass(ExtractCounts.ReducerClass.class);
            job2.setMapOutputKeyClass(DecadeText.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            Path op2 = new Path(output2);
            op2.getFileSystem(conf).delete(op2, true);

            FileInputFormat.addInputPath(job2, op1);
            FileOutputFormat.setOutputPath(job2, op2);


            Job job3 = new Job(conf, "mapReduce.ExtractLogRatio");
            job3.setJarByClass(ExtractLogRatio.class);

            job3.setMapperClass(ExtractLogRatio.MapperClass.class);
            job3.setPartitionerClass(ExtractLogRatio.PartitionerClass.class);
            //job.setCombinerClass(ExtractLogRatio.ReducerClass.class);
            job3.setReducerClass(ExtractLogRatio.ReducerClass.class);
            job3.setMapOutputKeyClass(DecadeText.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(DecadeText.class);
            job3.setOutputValueClass(DoubleWritable.class);
            job3.setInputFormatClass(TextInputFormat.class);
            job3.setOutputFormatClass(TextOutputFormat.class);
            Path op3 = new Path(output3);
            op3.getFileSystem(conf).delete(op3, true);

            FileInputFormat.addInputPath(job3, op2);
            FileOutputFormat.setOutputPath(job3, op3);

            conf.set("top", String.valueOf(100));

            Job job4 = Job.getInstance(conf, "mapReduce.extractTopCollocations");
            job4.setJarByClass(ExtractTopCollocations.class);

            job4.setMapperClass(ExtractTopCollocations.MapperClass.class);
            job4.setPartitionerClass(ExtractTopCollocations.PartitionerClass.class);
            job4.setSortComparatorClass(DecadeCountComparator.class);
            //job.setCombinerClass(ExtractTopCollocations.ReducerClass.class);
            job4.setReducerClass(ExtractTopCollocations.ReducerClass.class);

            job4.setMapOutputKeyClass(DecadeCount.class);
            job4.setMapOutputValueClass(Text.class);

            job4.setOutputKeyClass(DecadeText.class);
            job4.setOutputValueClass(DoubleWritable.class);

            job4.setInputFormatClass(TextInputFormat.class);
            job4.setOutputFormatClass(TextOutputFormat.class);

            Path op4 = new Path(output);
            op4.getFileSystem(conf).delete(op4, true);

            FileInputFormat.addInputPath(job4, op3);
            FileOutputFormat.setOutputPath(job4, op4);

            ControlledJob controlledJob1 = new ControlledJob(job1, new LinkedList<ControlledJob>());
            ControlledJob controlledJob2 = new ControlledJob(job2, new LinkedList<ControlledJob>());
            ControlledJob controlledJob3 = new ControlledJob(job3, new LinkedList<ControlledJob>());
            ControlledJob controlledJob4 = new ControlledJob(job4, new LinkedList<ControlledJob>());

            controlledJob2.addDependingJob(controlledJob1);
            controlledJob3.addDependingJob(controlledJob2);
            controlledJob4.addDependingJob(controlledJob3);

            JobControl jc = new JobControl("JC");
            jc.addJob(controlledJob1);
            jc.addJob(controlledJob2);
            jc.addJob(controlledJob3);
            jc.addJob(controlledJob4);
            Thread runJControl = new Thread(jc);
            runJControl.start();
            while (!jc.allFinished()) {
                Thread.sleep(5000);
            }
            System.exit(1);

            jc.run();
        } catch (Exception ignored) {

        }

    }
}
