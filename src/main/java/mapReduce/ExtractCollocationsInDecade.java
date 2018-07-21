package mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import writeableClasses.DecadeText;

public class ExtractCollocationsInDecade {
        public static class MapperClass extends Mapper<LongWritable, Text, DecadeText, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 4) { return; }
            String ngram = fields[0];
            String year = fields[1];
            String count= fields[2];
            String[] ngram_words = ngram.split(" ");
            if (ngram_words.length != 2) { return; }
            String decade = year.substring(0,3) + "0";
            context.write(new DecadeText(decade ,ngram), new IntWritable(Integer.valueOf(count)));
            }
        }

    public static class ReducerClass extends Reducer<DecadeText,IntWritable,DecadeText,IntWritable> {
        @Override
        public void reduce(DecadeText key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<DecadeText, IntWritable> {
        @Override
        public int getPartition(DecadeText key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("starting...");
        String input = args[0];
        String output = args[1];
        System.out.println(input);
        System.out.println(output);

        Configuration conf = new Configuration();

        Job job = new Job(conf, "extractCollocationsInDecade");
        job.setJarByClass(ExtractCollocationsInDecade.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DecadeText.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(DecadeText.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path op = new Path(output);
        op.getFileSystem(conf).delete(op,true);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, op);
        System.out.println("submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
