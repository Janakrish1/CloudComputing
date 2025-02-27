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

public class Part3Seven {

    // Mapper Class: Emits ("404", 1) for every log line with a 404 status code.
    public static class NotFoundMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        // We use a constant key "404" for counting 404 status occurrences.
        private final static Text statusKey = new Text("404");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Split the log line by double quotes to isolate the request section.
            // A typical log line:
            // 96.32.128.5 - - [15/Jul/2009:15:50:35 -0700] "GET /some/path HTTP/1.1" 404 7218
            String[] parts = line.split("\"");
            if (parts.length > 2) {
                // After the request part, the remaining text contains status code and bytes.
                // For example: " 404 7218"
                String afterRequest = parts[2].trim();
                String[] tokens = afterRequest.split(" ");
                if (tokens.length > 0) {
                    String status = tokens[0];
                    if ("404".equals(status)) {
                        context.write(statusKey, one);
                    }
                }
            }
        }
    }

    // Reducer Class: Sums all counts for the "404" key.
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
            System.err.println("Usage: NotFoundCount <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "404 Not Found Count");

        job.setJarByClass(Part3Seven.class);
        job.setMapperClass(NotFoundMapper.class);
        // Removing combiner as per request:
        // job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
