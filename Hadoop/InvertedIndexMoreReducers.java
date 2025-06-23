package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexMoreReducers {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordAndFile = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase().replaceAll("[^a-z0-9 ]", " ");
            String[] words = line.split("\\s+");

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            for(String word : words) {
                if(!word.isEmpty()) {
                    wordAndFile.set(word + "@" + fileName);
                    context.write(wordAndFile, one);
                }
            }
        }
    }

    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Map<String, List<String>> index = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) sum += val.get();

            String[] parts = key.toString().split("@");
            if(parts.length != 2) return;
            String word = parts[0];
            String file = parts[1];
            String fileCount = file + ":" + sum;

            if(!index.containsKey(word)){
                index.put(word, new ArrayList<>());
            }
            index.get(word).add(fileCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, List<String>> entry : index.entrySet()){
                String word = entry.getKey();
                String fileCounts = String.join("\t", entry.getValue());
                context.write(new Text(word), new Text(fileCounts));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndexMoreReducers.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);    // Uncomment to set number of reducers
        // job.setNumReduceTasks(4);    // Uncomment to set number of reducers
        // job.setNumReduceTasks(8);    // Uncomment to set number of reducers

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}