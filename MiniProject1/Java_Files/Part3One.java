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

public class Part3One {

    // Mapper Class
    public static class HitMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        // We emit a constant key "hits" for every valid hit
        private final Text hitKey = new Text("hits");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each log line is in Common Log Format.
            // Example: 10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /images/smilies/smile.png HTTP/1.1" 200 10469
            // We split by quotes to extract the request portion.
            String line = value.toString();
            String[] parts = line.split("\"");
            if (parts.length > 1) {
                // The request is typically in parts[1], e.g., GET /images/smilies/smile.png HTTP/1.1
                String request = parts[1];
                String[] reqParts = request.split(" ");
                if (reqParts.length > 1) {
                    String path = reqParts[1];
                    // Check if the request path starts with "/images/smilies/"
                    if (path.startsWith("/images/smilies/")) {
                        context.write(hitKey, one);
                    }
                }
            }
        }
    }

    // Reducer Class
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
            System.err.println("Usage: ImageSmiliesHitCount <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "images smilies hit count");

        job.setJarByClass(Part3One.class);
        job.setMapperClass(HitMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
