import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.log4j.BasicConfigurator;


public class RelationAlgebra {
    private static class SelectionMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            if (Integer.parseInt(tuple[2]) == 18)
                context.write(new IntWritable(1), value);
            else if (Integer.parseInt(tuple[2]) < 18)
                context.write(new IntWritable(2), value);
        }
    }

    //2.1 在Ra.txt上选择age=18的记录； 在Ra.txt上选择age<18的记录
    private static class SelectionReducer
            extends Reducer<IntWritable, Text, Text, NullWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.get() == 1) {
                context.write(new Text("Ra.txt上age = 18的记录有："), NullWritable.get());
                for (Text val : values)
                    context.write(val, NullWritable.get());
                context.write(new Text(" "), NullWritable.get());
            } else
                context.write(new Text("Ra.txt上age < 18的记录有："), NullWritable.get());
            for (Text val : values)
                context.write(val, NullWritable.get());
            context.write(new Text(" "), NullWritable.get());
        }
    }

    //2.2 在Ra.txt上对属性name进行投影
    private static class ProjectionMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            context.write(new IntWritable(1), new Text(tuple[1]));
        }
    }

    private static class ProjectionReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(new Text("在Ra.txt上对属性name进行投影："), NullWritable.get());
            for (Text val : values)
                context.write(val, NullWritable.get());
            context.write(new Text(" "), NullWritable.get());
        }
    }

    //2.3 求Ra1和Ra2的并集
    //2.4 求Ra1和Ra2的交集
    //2.5 求Ra2 - Ra1
    private static class SetOperationsMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.contains("1"))
                context.write(new IntWritable(1), new Text("1:" + value.toString()));
            else if (fileName.contains("2"))
                context.write(new IntWritable(1), new Text("2:" + value.toString()));
        }
    }

    private static class SetOperationsReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(new Text("进行关系操作："), NullWritable.get());
            Set<Text> set1 = new HashSet<Text>();
            Set<Text> set2 = new HashSet<Text>();
            for (Text val : values) {
                String[] tuple = val.toString().split(":");
                if (tuple[0].equals("1"))
                    set1.add(new Text(tuple[1]));
                else
                    set2.add(new Text(tuple[1]));
            }

            Set<Text> result = new HashSet<Text>(set1);
            result.retainAll(set2);
            context.write(new Text("交集："), NullWritable.get());
            for (Text str : result)
                context.write(str, NullWritable.get());
            context.write(new Text(" "), NullWritable.get());

            result.clear();
            result.addAll(set2);
            result.removeAll(set1);
            context.write(new Text("差集："), NullWritable.get());
            for (Text str : result)
                context.write(str, NullWritable.get());
            context.write(new Text(" "), NullWritable.get());

            result.clear();
            result.addAll(set1);
            result.addAll(set2);
            context.write(new Text("并集："), NullWritable.get());
            for (Text str : result)
                context.write(str, NullWritable.get());
            context.write(new Text(" "), NullWritable.get());
        }
    }

    //2.6 Ra和Rb在属性id上进行自然连接，要求最后的输出格式为(id, name, age, gender, weight, height)

    private static class ConnectionMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.contains("a")) {
                String[] tuple = value.toString().split(",", 2);
                context.write(new IntWritable(Integer.parseInt(tuple[0])), new Text("a:" + tuple[1]));
            } else if (fileName.contains("b")) {
                String[] tuple = value.toString().split(",", 2);
                context.write(new IntWritable(Integer.parseInt(tuple[0])), new Text("b:" + tuple[1]));
            }
        }
    }

    private static class ConnectionReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text[] arr = new Text[6]; // name, age, gender, weight, height
            arr[0] = new Text(key.toString());
            for (Text val : values) {
                String[] tuple = val.toString().split(":");
                if (tuple[0].equals("a")) //name, age, weight
                {
                    String[] tuples = tuple[1].split(",");
                    arr[1] = new Text(tuples[0]);
                    arr[2] = new Text(tuples[1]);
                    arr[4] = new Text(tuples[2]);
                } else { // gender, height
                    String[] tuples = tuple[1].split(",");
                    arr[3] = new Text(tuples[0]);
                    arr[5] = new Text(tuples[1]);
                }
            }
            Text out = new Text(arr[0].toString() + ',' + arr[1].toString() + ',' + arr[2].toString() + ',' + arr[3].toString() + ',' + arr[4].toString() + ',' + arr[5].toString());
            context.write(out, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        // BasicConfigurator.configure();

        Configuration conf1 = new Configuration();
        FileSystem fileSystem;
        fileSystem = FileSystem.get(new URI("Selection/"), conf1);
        if (fileSystem.exists(new Path("Selection/")))
            fileSystem.delete(new Path("Selection/"), true);
        Job job1 = Job.getInstance(conf1, "selection");
        job1.setJarByClass(RelationAlgebra.class);
        job1.setMapperClass(SelectionMapper.class);
        job1.setReducerClass(SelectionReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0] + "Ra.txt"));
        FileOutputFormat.setOutputPath(job1, new Path("Selection/"));

        Configuration conf2 = new Configuration();
        fileSystem = FileSystem.get(new URI("Projection/"), conf2);
        if (fileSystem.exists(new Path("Projection/")))
            fileSystem.delete(new Path("Projection/"), true);
        Job job2 = Job.getInstance(conf2, "projection");
        job2.setJarByClass(RelationAlgebra.class);
        job2.setMapperClass(ProjectionMapper.class);
        job2.setReducerClass(ProjectionReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0] + "Ra.txt"));
        FileOutputFormat.setOutputPath(job2, new Path("Projection/"));

        Configuration conf3 = new Configuration();
        fileSystem = FileSystem.get(new URI("SetOperations/"), conf3);
        if (fileSystem.exists(new Path("SetOperations/")))
            fileSystem.delete(new Path("SetOperations/"), true);
        Job job3 = Job.getInstance(conf3, "setOperations");
        job3.setJarByClass(RelationAlgebra.class);
        job3.setMapperClass(SetOperationsMapper.class);
        job3.setReducerClass(SetOperationsReducer.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job3, new Path(args[0] + "Ra1.txt"), new Path(args[0] + "Ra2.txt"));
        FileOutputFormat.setOutputPath(job3, new Path("SetOperations/"));

        Configuration conf4 = new Configuration();
        fileSystem = FileSystem.get(new URI("Connection/"), conf4);
        if (fileSystem.exists(new Path("Connection/")))
            fileSystem.delete(new Path("Connection/"), true);
        Job job4 = Job.getInstance(conf4, "connection");
        job4.setJarByClass(RelationAlgebra.class);
        job4.setMapperClass(ConnectionMapper.class);
        job4.setReducerClass(ConnectionReducer.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job4, new Path(args[0] + "Ra.txt"), new Path(args[0] + "Rb.txt"));
        FileOutputFormat.setOutputPath(job4, new Path("Connection/"));

        System.exit(job1.waitForCompletion(true) && job2.waitForCompletion(true) && job3.waitForCompletion(true) && job4.waitForCompletion(true) ? 0 : 1);
    }
}
