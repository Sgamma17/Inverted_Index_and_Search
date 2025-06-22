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

public class InvertedIndexMapperCombiner {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, Integer> wordCountMap;
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            wordCountMap = new HashMap<>();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase().replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");

            for (String word : words) {
                if (!word.isEmpty()) {
                    String compositeKey = word + "@" + fileName;
                    wordCountMap.put(compositeKey, wordCountMap.getOrDefault(compositeKey, 0) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            IntWritable count = new IntWritable();
            Text outputKey = new Text();

            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                outputKey.set(entry.getKey());
                count.set(entry.getValue());
                context.write(outputKey, count);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text currentWord = new Text();
    private String lastWord = "";
    private Map<String, Integer> fileMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] parts = key.toString().split("@");
        if (parts.length != 2) return;

        String word = parts[0];
        String filename = parts[1];

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Se la parola cambia, emetti la precedente
        if (!word.equals(lastWord) && !lastWord.equals("")) {
            emitCurrentWord(context);
            fileMap.clear();
        }

        lastWord = word;
        fileMap.put(filename, fileMap.getOrDefault(filename, 0) + sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        emitCurrentWord(context);
    }

    private void emitCurrentWord(Context context) throws IOException, InterruptedException {
        List<String> output = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fileMap.entrySet()) {
            output.add(entry.getKey() + ":" + entry.getValue());
        }
        currentWord.set(lastWord);
        context.write(currentWord, new Text(String.join("\t", output)));
    }
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index with Mapper Combiner");

        job.setJarByClass(InvertedIndexMapperCombiner.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
