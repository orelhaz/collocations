package mapReduce;

import java.io.IOException;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import writeableClasses.DecadeText;

public class ExtractLogRatio {
    public static class MapperClass extends Mapper<LongWritable, Text, DecadeText, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 6) {
                return;
            }
            String decade = fields[0];
            String ngram = fields[1];
            String word = fields[2];
            String ngramCount = fields[3];
            String wordCount = fields[4];
            String decadeCount = fields[5];

            context.write(new DecadeText(decade, String.join("\t", ngram, word)), new Text(String.join("\t", ngramCount, wordCount, decadeCount)));
        }
    }

    public static class ReducerClass extends Reducer<DecadeText, Text, DecadeText, DoubleWritable> {

        String ngramInMem = null;
        int firstWordCount = 0;

        @Override
        public void reduce(DecadeText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] fields = key.getText().toString().split("\t");
            if (fields.length < 2) {
                return;
            }
            String ngram = fields[0];

            for (Text value : values) {
                fields = value.toString().split("\t");
                int ngramCount = Integer.parseInt(fields[0]);
                int wordCount = Integer.parseInt(fields[1]);
                int decadeCount = Integer.parseInt(fields[2]);

                if (ngramInMem == null) {
                    ngramInMem = ngram;
                    firstWordCount = wordCount;
                    return;
                }

                double logLikelihoodRatio = ReducerClass.likelihoodRatio(firstWordCount, wordCount, ngramCount, decadeCount);
                //context.write(key, new DoubleWritable(logLikelihoodRatio));
                context.write(new DecadeText(key.getDecade().toString(), String.join("\t", ngram, String.valueOf(firstWordCount), String.valueOf(wordCount), String.valueOf(ngramCount), String.valueOf(decadeCount))), new DoubleWritable(logLikelihoodRatio));

                ngramInMem = null;
                firstWordCount = 0;
            }
        }

        /**
         * @param c1  - count of first word
         * @param c2  - count of second word
         * @param c12 - cound of 2-gram
         * @param N   - total number of words in decade
         * @return the log likelihood ratio
         */
        private static double likelihoodRatio(int c1, int c2, int c12, int N) {
            double p = c2 / N;
            double p1 = c12 / N;
            double p2 = (c2 - c12) / (N - c1);

            double b1 = new BinomialDistribution(c12, p).cumulativeProbability(c1);
            double b2 = new BinomialDistribution(c2 - c12, p).cumulativeProbability(N - c1);
            double b3 = new BinomialDistribution(c12, p1).cumulativeProbability(c1);
            double b4 = new BinomialDistribution(c2 - c12, p2).cumulativeProbability(N - c1);

            return Math.log(b1) + Math.log(b2) - Math.log(b3) - Math.log(b4);
        }
    }

    public static class PartitionerClass extends Partitioner<DecadeText, Text> {
        @Override
        public int getPartition(DecadeText key, Text value, int numPartitions) {
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

        Job job = new Job(conf, "mapReduce.ExtractLogRatio");
        job.setJarByClass(ExtractLogRatio.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DecadeText.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DecadeText.class);
        job.setOutputValueClass(DoubleWritable.class);
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
