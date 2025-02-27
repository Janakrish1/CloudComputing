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

public class Part3Four {

    // Mapper Class: Extracts the path from the log entry.
    public static class PathMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pathKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each log line is assumed to be in Common Log Format.
            // Example:
            // 10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /some/path HTTP/1.1" 200 10469
            // Split on double quotes to isolate the request part.
            String line = value.toString();
            String[] parts = line.split("\"");
            if (parts.length > 1) {
                // parts[1] contains: GET /some/path HTTP/1.1
                String request = parts[1];
                String[] reqParts = request.split(" ");
                if (reqParts.length > 1) {
                    // The path is the second token.
                    String path = reqParts[1];
                    pathKey.set(path);
                    context.write(pathKey, one);
                }
            }
        }
    }

    // Reducer Class: Finds the path with the maximum hit count.
    public static class MaxPathReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text maxPath = new Text();
        private int maxHits = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Update global maximum if this key has a higher count.
            if (sum > maxHits) {
                maxHits = sum;
                maxPath.set(key);
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the single record with the most hits.
            context.write(maxPath, new IntWritable(maxHits));
        }
    }

    
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("Usage: MostHitPath <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Hit Path Count");

        job.setJarByClass(Part3Four.class);
        job.setMapperClass(PathMapper.class);
        // Do not use a combiner here because we need all keys in one reducer for global maximum.
        job.setReducerClass(MaxPathReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Force a single reducer to compute the global maximum.
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
