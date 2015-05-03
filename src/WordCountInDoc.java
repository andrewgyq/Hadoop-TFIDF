import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by andrewgyq on 2015/3/22.
 */
public class WordCountInDoc {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text docName = new Text();
        private Text wordAndCount = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] wordAndDocCounter = value.toString().split("\t");
            String[] wordAndDoc = wordAndDocCounter[0].split("@");
            this.docName.set(wordAndDoc[1]);
            this.wordAndCount.set(wordAndDoc[0] + "=" + wordAndDocCounter[1]);
            context.write(this.docName, this.wordAndCount);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text wordAtDoc = new Text();
        private Text wordAvar = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sumOfWordsInDocument = 0;
            Map<String, Integer> tempCounter = new HashMap<String, Integer>();
            for (Text val : values) {
                // String[] wordCounter = val.toString().split("=");
                // tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
                // sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
                context.write(key, val);
            }
            
            // for (String wordKey : tempCounter.keySet()) {
            //     this.wordAtDoc.set(wordKey + "@" + key.toString());
            //     this.wordAvar.set(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument);
            //     context.write(this.wordAtDoc, this.wordAvar);
            // }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCountInDoc <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCountInDoc.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
