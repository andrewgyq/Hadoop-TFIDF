import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.text.DecimalFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
/**
 * Created by andrewgyq on 2015/3/17.
 */
public class TFIDF{
    private static final String OUTPUT_PATH = "temp1";

    private static final String OUTPUT_PATH_2 = "temp2";

    private static final String OUTPUT_PATH_3 = "temp3";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TFIDF <query file> <in> <out>");
            System.exit(3);
        }

        // get the number of documents  
        FileSystem fs = FileSystem.get(conf);
        final int docNumber = fs.listStatus(new Path(otherArgs[1])).length;
        
        Job job = new Job(conf, "TermFrequency");
        job.setJarByClass(TermFrequency.class);
        job.setMapperClass(TermFrequency.TokenizerMapper.class);
        job.setCombinerClass(TermFrequency.IntSumReducer.class);
        job.setReducerClass(TermFrequency.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true);

        Job job2 = new Job(conf, "WordCountInDoc");
        job2.setJarByClass(WordCountInDoc.class);
        job2.setMapperClass(WordCountInDoc.TokenizerMapper.class);
        job2.setReducerClass(WordCountInDoc.IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH_2));
        job2.waitForCompletion(true);

        conf.setInt("docNumber", docNumber);
        Job job3 = new Job(conf, "DocumentFrequencyTFIDF");
        job3.setJarByClass(DocumentFrequencyTFIDF.class);
        job3.setMapperClass(DocumentFrequencyTFIDF.TokenizerMapper.class);
        job3.setReducerClass(DocumentFrequencyTFIDF.IntSumReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH_2));
        FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_PATH_3));
        job3.waitForCompletion(true);

        conf.set("queryFile", otherArgs[0]);
        Job job4 = new Job(conf, "Query");
        job4.setJarByClass(Query.class);
        job4.setMapperClass(Query.TokenizerMapper.class);
        job4.setReducerClass(Query.IntSumReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(OUTPUT_PATH_3));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[2]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }

}

class TermFrequency {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // Get the name of the doc from the input-split in the context
            String docName = ((FileSplit) context.getInputSplit()).getPath().getName();

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                StringBuffer sb = new StringBuffer();
                String tmp = itr.nextToken().toLowerCase();
                sb.append(tmp.replaceAll("[\\pP‘’“”=]", ""));
                sb.append("@");
                sb.append(docName);
                this.word.set(sb.toString());
                context.write(this.word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}

class WordCountInDoc {
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
                String[] wordCounter = val.toString().split("=");
                tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
                sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
 
            }
            
            for (String wordKey : tempCounter.keySet()) {
                this.wordAtDoc.set(wordKey + "@" + key.toString());
                this.wordAvar.set(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument);
                context.write(this.wordAtDoc, this.wordAvar);
            }
        }
    }
}

class DocumentFrequencyTFIDF {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text wordAndDoc = new Text();
        private Text wordAndCounters = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] wordAndCounters = value.toString().split("\t");
            String[] wordAndDoc = wordAndCounters[0].split("@");  
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
            int numberOfDocumentsInCorpus = context.getConfiguration().getInt("docNumber", 0);;   
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
                this.tfidfCounts.set(DF.format(tfIdf));
                context.write(this.wordAtDocument, this.tfidfCounts);
            }
        }
    }
}

class Query {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private static List<String> queryList = new ArrayList<String>();
        private Text queryWord = new Text();
        private Text tfidfAndDoc = new Text();

        public void setup(Context context) throws IOException{
        	String queryFile = context.getConfiguration().get("queryFile");
            Path pt = new Path("/" + queryFile);//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            while (line != null){
                queryList.add(line);
                line = br.readLine();
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] wordDocAndTfidf = value.toString().split("\t");
            String[] wordAndDoc = wordDocAndTfidf[0].split("@");
            if(queryList.contains(wordAndDoc[0])){
                this.queryWord.set(new Text(wordAndDoc[0]));
                this.tfidfAndDoc.set(wordDocAndTfidf[1] + " " + wordAndDoc[1]);
                context.write(this.queryWord, this.tfidfAndDoc);
            }      
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {

        private Text queryWord = new Text();
        private Text queryresult = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double max = -1d;
            for (Text val : values) {
                String[] tfidfAndDoc = val.toString().split(" ");
                if(Double.valueOf(tfidfAndDoc[0]) > max){
                    max = Double.valueOf(tfidfAndDoc[0]);
                    this.queryWord.set(key);
                    this.queryresult.set(val);
                }
            }
            context.write(this.queryWord, this.queryresult);          
        }
    }
}

