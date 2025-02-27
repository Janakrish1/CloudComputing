import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Part3Ten {

    // Mapper Class: Emits ("data", bytes) for each log line on 16/Jan/2022 with status code 200.
    public static class DataMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static Text dataKey = new Text("data");
        private final static LongWritable bytesWritable = new LongWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Extract the date/time field between '[' and ']'
            int startBracket = line.indexOf('[');
            int endBracket = line.indexOf(']');
            if (startBracket == -1 || endBracket == -1) {
                return;
            }
            String dateTime = line.substring(startBracket + 1, endBracket);
            int colonIndex = dateTime.indexOf(':');
            if (colonIndex == -1) {
                return;
            }
            // Get the date part (e.g., "16/Jan/2022")
            String date = dateTime.substring(0, colonIndex);
            if (!"16/Jan/2022".equals(date)) {
                return;
            }
            
            // Split by double quotes to isolate the request.
            String[] parts = line.split("\"");
            if (parts.length < 3) {
                return;
            }
            // The part after the request (parts[2]) should contain: status code and bytes, then extra fields.
            String afterRequest = parts[2].trim();
            String[] tokens = afterRequest.split(" ");
            if (tokens.length < 2) {
                return;
            }
            // Check if status code is 200.
            String status = tokens[0];
            if (!"200".equals(status)) {
                return;
            }
            
            // Extract the bytes field. Skip if it is "-" (not available).
            String bytesStr = tokens[1];
            if (bytesStr.equals("-")) {
                return;
            }
            try {
                long bytes = Long.parseLong(bytesStr);
                bytesWritable.set(bytes);
                context.write(dataKey, bytesWritable);
            } catch (NumberFormatException e) {
                // Ignore lines with invalid byte numbers.
                return;
            }
        }
    }

    // Reducer Class: Sums all bytes for key "data" using LongWritable.
    public static class DataReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long totalBytes = 0;
            for (LongWritable val : values) {
                totalBytes += val.get();
            }
            context.write(key, new LongWritable(totalBytes));
        }
    }

   
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DataRequested200OnDate <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Requested with Status 200 on 16/Jan/2022");
        job.setJarByClass(Part3Ten.class);
        job.setMapperClass(DataMapper.class);
        // Do not set a combiner to ensure proper global aggregation.
        job.setReducerClass(DataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
