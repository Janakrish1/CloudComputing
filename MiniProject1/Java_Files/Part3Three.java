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

public class Part3Three {

    // Mapper Class: Extracts HTTP method from the request field in the log line.
    public static class MethodMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text methodKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each log line is in Common Log Format:
            // Example:
            // 10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /assets/js/lowpro.js HTTP/1.1" 200 10469
            // We split by double quotes to isolate the request portion.
            String line = value.toString();
            String[] parts = line.split("\"");
            if (parts.length > 1) {
                // parts[1] contains the request line, e.g.: GET /assets/js/lowpro.js HTTP/1.1
                String request = parts[1];
                String[] reqTokens = request.split(" ");
                if (reqTokens.length > 0) {
                    // The first token is the HTTP method (e.g., GET, POST, etc.)
                    String method = reqTokens[0];
                    methodKey.set(method);
                    context.write(methodKey, one);
                }
            }
        }
    }

    // Reducer Class: Sums up the counts for each HTTP method.
    public static class MethodReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: HttpRequestMethodCount <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HTTP Request Method Count");

        job.setJarByClass(Part3Three.class);
        job.setMapperClass(MethodMapper.class);
        job.setCombinerClass(MethodReducer.class);
        job.setReducerClass(MethodReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
