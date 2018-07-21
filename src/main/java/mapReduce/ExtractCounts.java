package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import writeableClasses.DecadeText;

import java.io.IOException;

public class ExtractCounts {
    public static class MapperClass extends Mapper<LongWritable, Text, DecadeText, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 4) return;
            String decade = fields[0];
            String word = fields[1];
            String ngram = fields[2];
            int count = Integer.parseInt(fields[3]);

            context.write(new DecadeText(decade,String.join("\t", word, ngram)), new IntWritable(count));
        }
    }

    public static class ReducerClass extends Reducer<DecadeText, IntWritable, Text, Text> {
        int wordSum = 0;
        int decadeSum = 0;

        @Override
        public void reduce(DecadeText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] fields = key.getText().toString().split("\t");
            String decade = key.getDecade().toString();
            String word = fields[0];
            String ngram = fields[1];

            for (IntWritable value : values) {

                if (word.equals(" ") && ngram.equals(" ")) { //decade sum row
                    decadeSum = value.get();
                    return;
                }

                if (ngram.equals(" ")) { // word sum row
                    wordSum = value.get();
                    return;
                }

                context.write(new Text(String.join("\t", decade, ngram, word)),
                        new Text(String.join("\t", value.toString(), String.valueOf(wordSum), String.valueOf(decadeSum))));
            }
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

        Job job = new Job(conf, "mapReduce.SplitWords");
        job.setJarByClass(ExtractCounts.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        // job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DecadeText.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path op = new Path(output);
        op.getFileSystem(conf).delete(op, true);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, op);
        System.out.println("submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}