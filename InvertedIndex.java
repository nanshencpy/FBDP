import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.log4j.BasicConfigurator;


public class InvertedIndex {

    static class MyText extends Text {

        MyText(String string) {
            this.set(string);
        }

        MyText() {

        }

        public int compareTo(BinaryComparable other) {
            StringBuilder s1 = new StringBuilder();
            StringBuilder s2 = new StringBuilder();
            for (int i = 0; i < this.getLength(); ++i)
                s1.append((char)this.getBytes()[i]);
            for (int j = 0; j < other.getLength(); ++j)
                s2.append((char)other.getBytes()[j]);
            String[] split1 = s1.toString().split("#");
            String[] split2 = s2.toString().split("#");
            if (!split1[0].equals(split2[0]))
                return split1[0].compareTo(split2[0]);
            else
                return Integer.parseInt(split1[1]) - Integer.parseInt(split2[1]);
        }
    }

    private static class MyMapper extends Mapper<Object, Text, MyText, Text> {
        MyText inValue = new MyText();
        Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                inValue.set(itr.nextToken() + "#1");
                outValue.set(fileName);
                context.write(inValue, outValue); // apple#1, file1.txt
            }
        }
    }


    private static class MyCombiner extends Reducer<MyText, Text, MyText, Text> {
        public void reduce(MyText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("#");
            String outValue = "";
            int sum = 0;
            for (Text val : values) {
                sum += 1;
                outValue = val.toString();
            }
            context.write(new MyText(split[0] + '#' + sum), new Text(outValue));
        }
    }

    public static class MyPartitioner extends HashPartitioner<MyText, Text> {
        public int getPartition(MyText key, Text value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return (term.hashCode() & 2147483647) % numReduceTasks;

        }
    }

    private static class MyReducer extends Reducer<MyText, Text, Text, Text> {
        int sum = 0;
        Text currentWord = new Text("");
        String outValue = "";

        public void reduce(MyText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("#");
            if (!currentWord.toString().equals(split[0]) && !currentWord.toString().equals("")) {
                context.write(currentWord, new Text(outValue));
                outValue = "";
                currentWord.set(split[0]);
            }
            currentWord.set(split[0]);
            for (Text val : values) {
                sum += Integer.parseInt(split[1]);
                outValue += '<' + split[1] + ',' + val + "> ";
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(currentWord, new Text(outValue));
        }
    }


    public static void main(String[] args) throws Exception {
        // BasicConfigurator.configure();
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("output/"), conf);
        if (fileSystem.exists(new Path("output/")))
            fileSystem.delete(new Path("output/"), true);

        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setCombinerClass(MyCombiner.class);
        job.setPartitionerClass(MyPartitioner.class);
        // job.setNumReduceTasks(3);

        ChainMapper.addMapper(job, MyMapper.class, Object.class, Text.class, MyText.class, Text.class, conf);
        ChainReducer.setReducer(job, MyReducer.class, MyText.class, Text.class, Text.class, Text.class, conf);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


