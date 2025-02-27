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

public class NGram {

    // Mapper Class for n-grams
    public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ngram = new Text();
        private int n;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Retrieve the n value from configuration; default to 2 if not provided
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 2);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Get the input line; optionally remove whitespace if desired
            String line = value.toString();

            // Generate and write every n-gram in the line
            for (int i = 0; i <= line.length() - n; i++) {
                String gram = line.substring(i, i + n);
                ngram.set(gram);
                context.write(ngram, one);
            }
        }
    }

    // Reducer Class remains the same, summing counts for each n-gram
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: HadoopMapReduceTemplate <input path> <output path> <n-gram size>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Set the n-gram size (n) as a configuration parameter
        conf.setInt("n", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(NGram.class);
        job.setMapperClass(NGramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
