import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

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
// import org.apache.log4j.BasicConfigurator;


public class MatrixMultiply {
    private static int rowM;
    private static int columnN;

    private static class Coordinate implements WritableComparable<Coordinate> {
        private IntWritable x;
        private DoubleWritable y;

        Coordinate() {
            x = new IntWritable();
            y = new DoubleWritable();
        }

        void set(int a, double b) {
            x = new IntWritable(a);
            y = new DoubleWritable(b);
        }

        IntWritable getX() {
            return x;
        }

        DoubleWritable getY() {
            return y;
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            x.readFields(dataInput);
            y.readFields(dataInput);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            x.write(dataOutput);
            y.write(dataOutput);
        }

        @Override
        public int compareTo(Coordinate c) {
            if (x.compareTo(c.x) == 0)
                return y.compareTo(c.y);
            else
                return x.compareTo(c.x);
        }

        @Override
        public boolean equals(Object c) {
            if (c instanceof Coordinate) {
                Coordinate other = (Coordinate) c;
                return x.equals(other.x) && y.equals(other.y);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return x.hashCode() + y.hashCode();
        }

    }

    private static class MyMapper extends Mapper<Object, Text, Coordinate, Coordinate> {


        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            rowM = Integer.parseInt(conf.get("rowM"));
            columnN = Integer.parseInt(conf.get("columnN"));
        }

        // M的每个值传colN次，N的每个值传rowM次，
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 按逗号和换行符拆分
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String[] tuple = value.toString().split(",");
            String[] tuples = tuple[1].split("\t");
            Coordinate map_key = new Coordinate();
            Coordinate map_value = new Coordinate();
            // 分别处理M和N的文件
            if (fileName.contains("M")) {
                int i = Integer.parseInt(tuple[0]);
                int j = Integer.parseInt(tuples[0]);
                double Mij = Double.parseDouble(tuples[1]);
                for (double k = 1; k < columnN + 1; k++) {
                    map_key.set(i, k);
                    map_value.set(j, Mij);
                    context.write(map_key, map_value);
                }
            } else if (fileName.contains("N")) {
                int j = Integer.parseInt(tuple[0]);
                double k = Double.parseDouble(tuples[0]);
                double Njk = Double.parseDouble(tuples[1]);
                for (int i = 1; i < rowM + 1; i++) {
                    map_key.set(i, k);
                    map_value.set(j, Njk);
                    context.write(map_key, map_value);
                }
            }
        }
    }

    private static class MyReducer extends Reducer<Coordinate, Coordinate, Text, Text> {
        public void reduce(Coordinate key, Iterable<Coordinate> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int sizeMN = Integer.parseInt(conf.get("sizeMN"));
            // 将key相同的值都存到arr里
            Coordinate[] arr = new Coordinate[2 * sizeMN];
            int i = 0;
            for (Coordinate val : values) {
                arr[i] = new Coordinate();
                arr[i].x = new IntWritable(val.getX().get());
                arr[i].y = new DoubleWritable(val.getY().get());
                i++;
            }
            Arrays.sort(arr);
            int sum = 0;
            for (int j = 0; j < 2 * sizeMN; j += 2)
                sum += arr[j].getY().get() * arr[j + 1].getY().get();
            Text map_key = new Text("(" + key.getX().get() + "," + (int)key.getY().get() + ") ");
            Text map_value = new Text(String.valueOf(sum));
            context.write(map_key, map_value);
        }
    }

    // M_3_4 N_4_2 output/
    public static void main(String[] args) throws Exception {
        // BasicConfigurator.configure();
        String[] infoTupleM = args[0].split("_");
        int rowM = Integer.parseInt(infoTupleM[1]); // 3
        int sizeMN = Integer.parseInt(infoTupleM[2]); // 4
        String[] infoTupleN = args[1].split("_");
        int columnN = Integer.parseInt(infoTupleN[2]); // 2
        Configuration conf = new Configuration();
        // 删除文件路径
        FileSystem fileSystem = FileSystem.get(new URI("output/"), conf);
        if (fileSystem.exists(new Path("output/")))
            fileSystem.delete(new Path("output/"), true);
        conf.setInt("rowM", rowM);
        conf.setInt("sizeMN", sizeMN);
        conf.setInt("columnN", columnN);
        Job job = Job.getInstance(conf, "matrix multiply");
        job.setJarByClass(MatrixMultiply.class);

        ChainMapper.addMapper(job, MyMapper.class, Object.class, Text.class, Coordinate.class, Coordinate.class, conf);
        ChainReducer.setReducer(job, MyReducer.class, Coordinate.class, Coordinate.class, Text.class, Text.class, conf);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
