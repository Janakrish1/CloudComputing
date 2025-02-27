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

public class Part3Five {

    // Mapper Class: Extracts the client IP from each log line.
    public static class IPMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ipKey = new Text();

        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            // Each log line is in Common Log Format.
            // Example:
            // 96.32.128.5 - - [15/Jul/2009:15:50:35 -0700] "GET /assets/js/lowpro.js HTTP/1.1" 200 10469
            String line = value.toString();
            String[] tokens = line.split(" ");
            if (tokens.length > 0) {
                String ip = tokens[0];  // The first token is the client IP.
                ipKey.set(ip);
                context.write(ipKey, one);
            }
        }
    }

    // Reducer Class: Sums all counts for each IP and tracks the IP with the maximum count.
    public static class MaxIPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text maxIP = new Text();
        private int maxCount = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Update global maximum if this IP has more accesses.
            if (sum > maxCount) {
                maxCount = sum;
                maxIP.set(key);
            }
        }
        
        @Override
        protected void cleanup(Context context) 
                throws IOException, InterruptedException {
            // Emit the IP with the highest number of accesses.
            context.write(maxIP, new IntWritable(maxCount));
        }
    }

    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MostAccessIP <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Access IP Count");

        job.setJarByClass(Part3Five.class);
        job.setMapperClass(IPMapper.class);
        // Do not use a combiner here since we need to compute the global maximum.
        job.setReducerClass(MaxIPReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Force a single reducer to compute the global maximum.
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
