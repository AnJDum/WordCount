import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeMap;

public class WordCount {

    public static Map<Integer, Double> Probs = new TreeMap<Integer, Double>();

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        static enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    if(fileName.equals("stop-word-list.txt")) {
                        patternsToSkip.add("\\b" + pattern + "\\b");
                    }
                    else{
                        patternsToSkip.add(pattern);
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String nextWord = itr.nextToken();
                if (nextWord.length() < 3) {
                    continue;
                }
                word.set(nextWord);
                context.write(word, one);
                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class FileTokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        static enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    if(fileName.equals("stop-word-list.txt")) {
                        patternsToSkip.add("\\b" + pattern + "\\b");
                    }
                    else{
                        patternsToSkip.add(pattern);
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, " ");
            }
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String nextWord = itr.nextToken();
                if (nextWord.length() < 3) {
                    continue;
                }
                word.set(nextWord+"%"+fileName);
                context.write(word, one);
                Counter counter = context.getCounter(
                        TokenizerMapper.CountersEnum.class.getName(),
                        TokenizerMapper.CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
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


    public static class FileSortReducer
            extends Reducer<Text,IntWritable,Text,NullWritable>{
        private MultipleOutputs<Text,NullWritable> many;

        protected void setup(Context context) throws IOException,InterruptedException {
            many = new MultipleOutputs<Text, NullWritable>(context);
        }
        protected void cleanup(Context context) throws IOException,InterruptedException {
            many.close();
        }

        private Text result = new Text();
        private HashMap<String, Integer> map = new HashMap<>();

        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val: values){
                String docId = val.toString().split("#")[1];
                docId = docId.substring(0, docId.length()-4);
                docId = docId.replaceAll("-", "");
                String oneWord = val.toString().split("#")[0];
                int sum = map.values().stream().mapToInt(i->i).sum();
                int num = map.getOrDefault(docId, 0);
                if(sum == 4000){break;}
                if(num == 100){continue;}
                else {map.put(docId, num);num = num + 1; }
                many.write(docId, new Text(num+": "+oneWord.toString()+", "+key), NullWritable.get() );
            }
        }
    }

    private static class IntWritableDecreasingComparator
            extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static  class RankOutputFormat<K,V>
            extends TextOutputFormat<K,V> {
        public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            String keyValueSeparator = conf.get(SEPARATOR, ",");
            CompressionCodec codec = null;
            String extension = "";
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
                codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                extension = codec.getDefaultExtension();
            }

            Path file = this.getDefaultWorkFile(job, extension);
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(file, false);
            return isCompressed ? new WordCount.RankOutputFormat.LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator) : new WordCount.RankOutputFormat.LineRecordWriter(fileOut, keyValueSeparator);
        }
        protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
            private static final byte[] NEWLINE;
            protected DataOutputStream out;
            private final byte[] keyValueSeparator;

            public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
                this.out = out;
                this.keyValueSeparator = keyValueSeparator.getBytes(StandardCharsets.UTF_8);
            }

            public LineRecordWriter(DataOutputStream out) {
                this(out, "\t");
            }

            private void writeObject(Object o) throws IOException {
                if (o instanceof Text) {
                    Text to = (Text)o;
                    this.out.write(to.getBytes(), 0, to.getLength());
                } else {
                    this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
                }

            }
            public  static IntWritable rank=new IntWritable(1);

            public synchronized void write(K key, V value) throws IOException {
                boolean nullKey = key == null || key instanceof NullWritable;
                boolean nullValue = value == null || value instanceof NullWritable;
                if ((!nullKey || !nullValue)&&rank.compareTo(new IntWritable(100))<=0) {
                    this.writeObject(rank);
                    this.writeObject(":");
                    if (!nullValue) {
                        this.writeObject(value);
                    }
                    if (!nullKey && !nullValue) {
                        this.out.write(this.keyValueSeparator);
                    }
                    if (!nullKey) {
                        this.writeObject(key);
                    }

                    this.out.write(NEWLINE);
                }
                rank.set(rank.get()+1);
            }

            public synchronized void close(TaskAttemptContext context) throws IOException {
                this.out.close();
            }

            static {
                NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
            }
        }

    }

    public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

            String[] remainingArgs = optionParser.getRemainingArgs();
            if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
                System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
                System.exit(2);
            }

            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);


            List<String> otherArgs = new ArrayList<String>();
            job.addCacheFile(new Path(remainingArgs[2]).toUri());
            job.addCacheFile(new Path(remainingArgs[3]).toUri());
            job.getConfiguration().setBoolean("wordcount.skip.patterns", true);

            for (int i = 0; i < remainingArgs.length; ++i) {
                if ("-skip".equals(remainingArgs[i])) {
                    job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                } else {
                    otherArgs.add(remainingArgs[i]);
                }
            }
            FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        Path temp1=new Path("all");
        FileOutputFormat.setOutputPath(job,temp1);
        System.out.println("temp1\n");
        if(job.waitForCompletion(true))
        {
            System.out.println("job1\n");
            //sort job

            Job sortJob = Job.getInstance(conf, "sort");
            sortJob.setJarByClass(WordCount.class);
            FileInputFormat.addInputPath(sortJob, temp1);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            sortJob.setMapperClass(InverseMapper.class);
            Path temp2=new Path("all-sort");
            FileOutputFormat.setOutputPath(job,temp2);
            System.out.println("temp2\n");
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            sortJob.setOutputFormatClass(RankOutputFormat.class);
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            if(sortJob.waitForCompletion(true))
            {
                System.out.println("job2\n");
            }

        }

        // file by file
        Job fileJob = Job.getInstance(conf, "word count");
        fileJob.setJarByClass(WordCount.class);
        FileInputFormat.addInputPath(fileJob, new Path(otherArgs.get(0)));
        fileJob.setMapperClass(FileTokenizerMapper.class);
        fileJob.setCombinerClass(IntSumReducer.class);
        fileJob.setReducerClass(IntSumReducer.class);
        fileJob.setOutputKeyClass(Text.class);
        fileJob.setOutputValueClass(IntWritable.class);
        fileJob.addCacheFile(new Path(remainingArgs[2]).toUri());
        fileJob.addCacheFile(new Path(remainingArgs[3]).toUri());
        fileJob.getConfiguration().setBoolean("wordcount.skip.patterns", true);

        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                fileJob.addCacheFile(new Path(remainingArgs[++i]).toUri());
                fileJob.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(fileJob, new Path(otherArgs.get(0)));
        Path temp3=new Path("file");
        FileOutputFormat.setOutputPath(fileJob,temp3);
        fileJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        if(fileJob.waitForCompletion(true)) {
            System.out.println("temp3\n");
            //file sort
            Job fileSortJob = Job.getInstance(conf, "sort");
            fileSortJob.setJarByClass(WordCount.class);
            FileInputFormat.addInputPath(fileSortJob, temp3);
            fileSortJob.setInputFormatClass(SequenceFileInputFormat.class);
            fileSortJob.setMapperClass(InverseMapper.class);
            fileSortJob.setReducerClass(FileSortReducer.class);
            Path temp4=new Path("file-sort");
            FileOutputFormat.setOutputPath(fileSortJob,temp4);
            List<String> fileList = Arrays.asList("shakespearealls11", "shakespeareantony23", "shakespeareas12",
                    "shakespearecomedy7", "shakespearecoriolanus24", "shakespearecymbeline17", "shakespearefirst51",
                    "shakespearehamlet25", "shakespearejulius26", "shakespeareking45", "shakespearelife54",
                    "shakespearelife55", "shakespearelife56", "shakespearelovers62", "shakespeareloves8",
                    "shakespearemacbeth46", "shakespearemeasure13", "shakespearemerchant5", "shakespearemerry15",
                    "shakespearemidsummer16", "shakespearemuch3", "shakespeareothello47", "shakespearepericles21",
                    "shakespearerape61", "shakespeareromeo48", "shakespearesecond52", "shakespearesonnets59",
                    "shakespearesonnets", "shakespearetaming2", "shakespearetempest4", "shakespearethird53",
                    "shakespearetimon49", "shakespearetitus50", "shakespearetragedy57", "shakespearetragedy58",
                    "shakespearetroilus22", "shakespearetwelfth20", "shakespearetwo18", "shakespearevenus60",
                    "shakespearewinters19");
            for (String fileName : fileList) {
                MultipleOutputs.addNamedOutput(fileSortJob, fileName, TextOutputFormat.class,Text.class, NullWritable.class);
            }
            fileSortJob.setOutputKeyClass(IntWritable.class);
            fileSortJob.setOutputValueClass(Text.class);
            fileSortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            if(fileSortJob.waitForCompletion(true))
            {
                System.out.println("temp4\n");
            }
            fileSortJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}