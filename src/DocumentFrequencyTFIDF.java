import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by andrewgyq on 2015/3/22.
 */
public class DocumentFrequencyTFIDF {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text wordAndDoc = new Text();
        private Text wordAndCounters = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] wordAndCounters = value.toString().split("\t");
            String[] wordAndDoc = wordAndCounters[0].split("@");  //3/1500
            this.wordAndDoc.set(new Text(wordAndDoc[0]));
            this.wordAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
            context.write(this.wordAndDoc, this.wordAndCounters);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private static final DecimalFormat DF = new DecimalFormat("###.########");
        private Text wordAtDocument = new Text();
        private Text tfidfCounts = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // get the number of documents indirectly from the file-system
            int numberOfDocumentsInCorpus = 24;
            // total frequency of this word
            int numberOfDocumentsInCorpusWhereKeyAppears = 0;
            Map<String, String> tempFrequencies = new HashMap<String, String>();
            for (Text val : values) {
                String[] documentAndFrequencies = val.toString().split("=");
                // in case the counter of the words is > 0
                if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
                    numberOfDocumentsInCorpusWhereKeyAppears++;
                }
                tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
            }
            for (String document : tempFrequencies.keySet()) {
                String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");

                // Term frequency is the quotient of the number occurrences of the term in document and the total
                // number of terms in document
                double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                        / Double.valueOf(wordFrequenceAndTotalWords[1]));

                // inverse document frequency quotient between the number of docs in corpus and number of docs the
                // term appears Normalize the value in case the number of appearances is 0.
                double idf = Math.log10((double) numberOfDocumentsInCorpus /
                        (double) ((numberOfDocumentsInCorpusWhereKeyAppears == 0 ? 1 : 0) +
                                numberOfDocumentsInCorpusWhereKeyAppears));

                double tfIdf = tf * idf;

                this.wordAtDocument.set(key + "@" + document);
                this.tfidfCounts.set("[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
                        + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
                        + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]");

                context.write(this.wordAtDocument, this.tfidfCounts);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TFIDF <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "TFIDF");
        job.setJarByClass(DocumentFrequencyTFIDF.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
