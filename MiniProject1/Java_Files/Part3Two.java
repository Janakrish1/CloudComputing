import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Part3Two {

    // Mapper Class: Emit (IP, 1) if the log's IP equals "96.32.128.5"
    public static class IPMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text ipKey = new Text("96.32.128.5");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each log line follows the Common Log Format:
            // %h %l %u %t \"%r\" %>s %b
            // Example:
            // 96.32.128.5 - - [15/Jul/2009:15:50:35 -0700] "GET /some/path HTTP/1.1" 200 10469
            String line = value.toString();
            // Split the line by whitespace; the first token is the client IP.
            String[] tokens = line.split(" ");
            if (tokens.length > 0) {
                String ip = tokens[0];
                if ("96.32.128.5".equals(ip)) {
                    // Emit the IP key and a count of 1
                    context.write(ipKey, one);
                }
            }
        }
    }

    // Reducer Class: Sum all counts for the key "96.32.128.5"
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

   
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: IPHitCount <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IP Hit Count");

        job.setJarByClass(Part3Two.class);
        job.setMapperClass(IPMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
