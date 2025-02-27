import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.Writable;
 
// Custom Writable to store (hitCount, totalBytes)
// Here, 'first' (hit count) remains an int, but 'second' (total bytes) is a long.
class IntPair implements Writable {
    private int first;      // access count
    private long second;    // total bytes
 
    // Default constructor is required
    public IntPair() {}
 
    public IntPair(int first, long second) {
        this.first = first;
        this.second = second;
    }
 
    public int getFirst() {
        return first;
    }
 
    public long getSecond() {
        return second;
    }
 
    public void setFirst(int first) {
        this.first = first;
    }
 
    public void setSecond(long second) {
        this.second = second;
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeLong(second);
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readLong();
    }
 
    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
 
public class Part3Nine {
 
    // Mapper Class: Emits (IP, (1, bytes)) for each log line.
    public static class IPMapper extends Mapper<Object, Text, Text, IntPair> {
        private Text ipKey = new Text();
        private IntPair pair = new IntPair();
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Extract the IP using space-splitting (IP is the first token)
            String[] spaceTokens = line.split(" ");
            if (spaceTokens.length < 7) {
                return; // skip malformed lines
            }
            String ip = spaceTokens[0];
            ipKey.set(ip);
            
            // To correctly get the bytes field, split by double quotes.
            // A typical log line (with extra fields) looks like:
            // 10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /path HTTP/1.1" 200 10469 extra1 extra2
            String[] quoteParts = line.split("\"");
            if (quoteParts.length < 3) {
                return; // not in expected format
            }
            // The part after the request is in quoteParts[2], e.g.: 200 10469 extra1 extra2
            String afterRequest = quoteParts[2].trim();
            String[] tokens = afterRequest.split(" ");
            if (tokens.length < 2) {
                return;
            }
            // tokens[0] is the status code (ignored here) and tokens[1] is the bytes field.
            String bytesStr = tokens[1];
            long bytes = 0;
            if (!bytesStr.equals("-")) {
                try {
                    bytes = Long.parseLong(bytesStr);
                } catch (NumberFormatException e) {
                    bytes = 0;
                }
            }
            pair.setFirst(1);
            pair.setSecond(bytes);
            context.write(ipKey, pair);
        }
    }
 
    // Reducer Class: Sums counts and bytes per IP, then outputs only the top 3 IPs.
    public static class TopIPReducer extends Reducer<Text, IntPair, Text, IntPair> {
        // Helper class to store the IP and its aggregated values.
        private static class IPRecord {
            String ip;
            int count;
            long bytes;
            public IPRecord(String ip, int count, long bytes) {
                this.ip = ip;
                this.count = count;
                this.bytes = bytes;
            }
        }
 
        // Use a min-heap to track the top 3 records (lowest count at the top).
        private PriorityQueue<IPRecord> topIPs = new PriorityQueue<>(3, new Comparator<IPRecord>() {
            public int compare(IPRecord a, IPRecord b) {
                return Integer.compare(a.count, b.count);
            }
        });
 
        public void reduce(Text key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            long totalBytes = 0;
            for (IntPair val : values) {
                totalCount += val.getFirst();
                totalBytes += val.getSecond();
            }
            IPRecord record = new IPRecord(key.toString(), totalCount, totalBytes);
            topIPs.add(record);
            if (topIPs.size() > 3) {
                topIPs.poll(); // remove the IP with the lowest count among the top ones.
            }
        }
 
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Transfer the top IP records to a list for sorting in descending order.
            List<IPRecord> topList = new ArrayList<>();
            while (!topIPs.isEmpty()) {
                topList.add(topIPs.poll());
            }
            // Sort in descending order (highest count first).
            Collections.sort(topList, new Comparator<IPRecord>() {
                public int compare(IPRecord a, IPRecord b) {
                    return Integer.compare(b.count, a.count);
                }
            });
            // Emit the top 3 IPs.
            for (IPRecord rec : topList) {
                context.write(new Text(rec.ip), new IntPair(rec.count, rec.bytes));
            }
        }
    }
 

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Part3Nine <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 3 IP Access Data Flow");
        job.setJarByClass(Part3Nine.class);
        job.setMapperClass(IPMapper.class);
        // Do not use a combiner because top-N is non-associative.
        job.setReducerClass(TopIPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Force a single reducer for global top-N.
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
