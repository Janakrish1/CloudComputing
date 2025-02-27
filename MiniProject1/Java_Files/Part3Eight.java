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

public class Part3Eight {

    // Mapper Class: For each log line, if the date is "19/Dec/2020", emit ("data", bytes)
    public static class DataMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text dataKey = new Text("data");
        private final static IntWritable bytesWritable = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Extract the date/time field (which is between '[' and ']')
            int startBracket = line.indexOf('[');
            int endBracket = line.indexOf(']');
            if (startBracket == -1 || endBracket == -1) {
                // Invalid format; skip this line.
                return;
            }
            // e.g., "19/Dec/2020:15:50:35 -0700"
            String dateTime = line.substring(startBracket + 1, endBracket);
            int colonIndex = dateTime.indexOf(':');
            if (colonIndex == -1) {
                return;
            }
            String date = dateTime.substring(0, colonIndex);  // should yield "19/Dec/2020"

            // Only process lines for the target date
            if (!"19/Dec/2020".equals(date)) {
                return;
            }

            // Now, extract the bytes field.
            // In Common Log Format the bytes field is the last token after the request.
            // One approach: split the line on the double quotes. parts[2] should then have status and bytes.
            String[] parts = line.split("\"");
            if (parts.length < 3) {
                return;
            }
            // parts[2] (trimmed) should look like: "200 10469"
            String afterRequest = parts[2].trim();
            String[] tokens = afterRequest.split(" ");
            if (tokens.length < 2) {
                return;
            }
            String bytesStr = tokens[1];
            if (bytesStr.equals("-")) {
                // No data  skip.
                return;
            }
            try {
                int bytes = Integer.parseInt(bytesStr);
                bytesWritable.set(bytes);
                context.write(dataKey, bytesWritable);
            } catch (NumberFormatException e) {
                // Skip lines with invalid byte numbers.
                return;
            }
        }
    }

    // Reducer Class: Sum all byte values for the key "data"
    public static class DataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalBytes = 0;
            for (IntWritable val : values) {
                totalBytes += val.get();
            }
            context.write(key, new IntWritable(totalBytes));
        }
    }

    
    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("Usage: DataRequestedOnDate <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Requested on 19/Dec/2020");

        job.setJarByClass(Part3Eight.class);
        job.setMapperClass(DataMapper.class);
        job.setCombinerClass(DataReducer.class);
        job.setReducerClass(DataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
