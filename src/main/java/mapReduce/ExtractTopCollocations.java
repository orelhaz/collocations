package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import writeableClasses.DecadeCount;
import writeableClasses.DecadeCountComparator;
import writeableClasses.DecadeText;

import java.io.IOException;

public class ExtractTopCollocations {
    public static class MapperClass extends Mapper<LongWritable, Text, DecadeCount, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 3) return;

            String decade = fields[0];
            String ngram = fields[1];
            Double lRatio = Double.valueOf(fields[2]);

            context.write(new DecadeCount(decade, new DoubleWritable(lRatio)), new Text(ngram));
        }
    }

    public static class ReducerClass extends Reducer<DecadeCount, Text, DecadeText, DoubleWritable> {
        int countInMem;
        String decadeInMem = null;

        @Override
        public void reduce(DecadeCount key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {

            int top = Integer.parseInt(context.getConfiguration().get("top", "100"));

            Text decade = key.getDecade();
            DoubleWritable lRatio = key.getCount();

            if (decadeInMem == null || !decade.toString().equals(decadeInMem)) {
                countInMem = 0;
                decadeInMem = decade.toString();
            }

            for (Text ngram : values) {
                if (countInMem >= top)
                    return;

                countInMem++;
                context.write(new DecadeText(decade.toString(),ngram.toString()), lRatio);

            }
        }
    }


    public static class PartitionerClass extends Partitioner<DecadeCount, Text> {
        @Override
        public int getPartition(DecadeCount key, Text value, int numPartitions) {
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
        conf.set("top", String.valueOf(100));

        Job job = Job.getInstance(conf, "extractTopCollocations");
        job.setJarByClass(ExtractTopCollocations.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setSortComparatorClass(DecadeCountComparator.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(DecadeCount.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DecadeText.class);
        job.setOutputValueClass(DoubleWritable.class);

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