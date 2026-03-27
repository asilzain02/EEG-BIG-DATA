package org.eeg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * EEG Feature Extraction using Hadoop MapReduce.
 *
 * Input format (CSV or text):
 *   timestamp,channel,value
 *   OR any row where last numeric token is treated as EEG signal value.
 *
 * Output format:
 *   GLOBAL	count,mean,min,max,stddev
 */
public class EEGFeatureExtraction {

    private static final double MIN_VALID = -10000.0;
    private static final double MAX_VALID = 10000.0;

    /** Mapper emits partial statistics for each valid EEG value. */
    public static class EEGMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text GLOBAL_KEY = new Text("GLOBAL");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // Skip header rows that contain alphabetic characters in key columns.
            if (line.toLowerCase().contains("timestamp") || line.toLowerCase().contains("channel")) {
                return;
            }

            Double eegValue = extractSignalValue(line);
            if (eegValue == null) {
                return;
            }

            if (eegValue < MIN_VALID || eegValue > MAX_VALID) {
                return;
            }

            // Emit tuple: count,sum,min,max,sumSquares
            double v = eegValue;
            String partial = String.format("1,%.10f,%.10f,%.10f,%.10f", v, v, v, v * v);
            context.write(GLOBAL_KEY, new Text(partial));
        }

        /**
         * Extract EEG value from line.
         * Strategy:
         * 1) If comma-separated, parse the last column as double.
         * 2) Else parse the last whitespace-delimited token.
         */
        private Double extractSignalValue(String line) {
            try {
                if (line.contains(",")) {
                    String[] cols = line.split(",");
                    String last = cols[cols.length - 1].trim();
                    return Double.parseDouble(last);
                } else {
                    StringTokenizer tokenizer = new StringTokenizer(line);
                    String last = null;
                    while (tokenizer.hasMoreTokens()) {
                        last = tokenizer.nextToken();
                    }
                    if (last != null) {
                        return Double.parseDouble(last.trim());
                    }
                }
            } catch (NumberFormatException ex) {
                // Ignore malformed numeric rows.
            }
            return null;
        }
    }

    /** Reducer aggregates all partial stats and computes final global features. */
    public static class EEGReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long count = 0L;
            double sum = 0.0;
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sumSquares = 0.0;

            for (Text t : values) {
                String[] parts = t.toString().split(",");
                if (parts.length != 5) {
                    continue;
                }

                long c = Long.parseLong(parts[0]);
                double s = Double.parseDouble(parts[1]);
                double mn = Double.parseDouble(parts[2]);
                double mx = Double.parseDouble(parts[3]);
                double ss = Double.parseDouble(parts[4]);

                count += c;
                sum += s;
                min = Math.min(min, mn);
                max = Math.max(max, mx);
                sumSquares += ss;
            }

            if (count == 0) {
                context.write(key, new Text("0,NaN,NaN,NaN,NaN"));
                return;
            }

            double mean = sum / count;
            double variance = (sumSquares / count) - (mean * mean);
            if (variance < 0) {
                variance = 0; // Numeric safety for floating-point errors.
            }
            double stddev = Math.sqrt(variance);

            String result = String.format("%d,%.6f,%.6f,%.6f,%.6f", count, mean, min, max, stddev);
            context.write(key, new Text(result));
        }
    }

    /** Driver class to configure and run MapReduce job. */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: EEGFeatureExtraction <input_hdfs_path> <output_hdfs_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EEG Feature Extraction");
        job.setJarByClass(EEGFeatureExtraction.class);

        job.setMapperClass(EEGMapper.class);
        job.setReducerClass(EEGReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
