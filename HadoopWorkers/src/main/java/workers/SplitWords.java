package workers;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import models.DecadeText;
import stopWords.EnglishStopWords;
import stopWords.HebrewStopWords;

import java.io.IOException;
import java.util.HashSet;

public class SplitWords {
    public static class MapperClass extends Mapper<LongWritable, Text, DecadeText, IntWritable> {

        public HashSet<String> _stopWords = new HashSet<String>();

        @Override
        public void setup(Context context) {

            String lang = context.getConfiguration().get("lang", "en");
            _stopWords = lang.equals("en") ? EnglishStopWords.GetStopWords() : HebrewStopWords.GetStopWords();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 4) {
                return;
            }
            String ngram = fields[0];
            String year = fields[1];
            int count = Integer.parseInt(fields[2]);
            String[] ngram_words = ngram.split(" ");
            if (ngram_words.length != 2) {
                return;
            }
            String decade = year.substring(0, 3) + "0";

            if (!_stopWords.contains(ngram_words[0]))
                context.write(new DecadeText(decade, ngram_words[0] + "\t" + ngram), new IntWritable(count));

            if (!_stopWords.contains(ngram_words[1]))
                context.write(new DecadeText(decade, ngram_words[1] + "\t" + ngram), new IntWritable(count));
        }
    }

    public static class ReducerClass extends Reducer<DecadeText, IntWritable, Text, Text> {
        String wordInMem = null;
        String decadeInMem = null;

        int wordSum = 0;
        int decadeSum = 0;


        /**
         * receives: key (decade, word, ngram)
         * yields: (decade, word, " "), sum
         */
        @Override
        public void reduce(DecadeText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] strValues = key.getvalue().toString().split("\t");

            // restoring the ngarm
            String ngramStr = strValues[1];
            for (int i = 2; i < strValues.length; i++) {
                ngramStr = ngramStr + "\t" + strValues[i];
            }

            Text word = new Text(strValues[0]);
            Text decade = key.getTag();
            Text ngarm = new Text(ngramStr);

            if (wordInMem == null) wordInMem = word.toString();
            if (decadeInMem == null) decadeInMem = decade.toString();

            if (!word.toString().equals(wordInMem)) {
                writeTotalWord(context);
                wordInMem = word.toString();
                wordSum = 0;
            }

            if (!decade.toString().equals(decadeInMem)) {
                writeTotalDecade(context);
                decadeInMem = decade.toString();
                decadeSum = 0;
            }

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(new Text(new DecadeText(decade, word).toString()), new Text(ngarm + "\t" + sum));
            wordSum += sum;
            decadeSum += sum;
        }

        private void writeTotalWord(Context context) throws IOException, InterruptedException {
            context.write(new Text(String.join("\t", decadeInMem, wordInMem, " ")), new Text(String.valueOf(wordSum)));
        }

        private void writeTotalDecade(Context context) throws IOException, InterruptedException {
            context.write(new Text(String.join("\t", decadeInMem, " ", " ")), new Text(String.valueOf(decadeSum)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (wordInMem != null) {
                writeTotalWord(context);
                writeTotalDecade(context);
            }
            super.cleanup(context);
        }
    }

    public static class PartitionerClass extends Partitioner<DecadeText, IntWritable> {
        @Override
        public int getPartition(DecadeText key, IntWritable value, int numPartitions) {
            int decadeRank = Integer.parseInt(key.getTag().toString().substring(0, 3)) - 50; // number from 2-150 ( decades 1520-2000)
            int where = decadeRank - (150 - (numPartitions - 1));
            if (where <= 0)
                return 0; // all of the earlier decades enter into the first partition
            return where; // the rest, numPartitions-1 decades will go each one to a specific partition
        }
    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "mapReduce.SplitWords");
        job.setJarByClass(SplitWords.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DecadeText.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path op = new Path(output);
        op.getFileSystem(conf).delete(op, true);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, op);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}