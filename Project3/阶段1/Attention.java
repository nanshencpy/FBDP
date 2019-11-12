import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Attention {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Attention");
        FileSystem fileSystem = FileSystem.get(new URI("attention/"), conf);
        if (fileSystem.exists(new Path("attention/")))
            fileSystem.delete(new Path("attention/"), true);
        job.setJarByClass(Attention.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("attention/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<Object, Text, Text, Text> {
        // 1是商品id，10是省份
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            context.write(new Text(tuple[10]), new Text(tuple[1]));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text val : values) {
                if (!map.containsKey(val.toString()))
                    map.put(val.toString(), 1);
                else
                    map.put(val.toString(), map.get(val.toString()) + 1);
            }
            context.write(key, new Text("前十热门关注产品有："));
            String sTemp = "";
            int temp = 0;
            for (int i = 0; i < 10 && !map.isEmpty(); ++i) {
                for (Map.Entry<String, Integer> entry : map.entrySet()) {
                    if (temp < entry.getValue()) {
                        sTemp = entry.getKey();
                        temp = entry.getValue();
                    }
                }
                map.remove(sTemp);
                context.write(new Text(sTemp), new Text("关注度：" + temp));
                sTemp = "";
                temp = 0;
            }
        }
    }
}