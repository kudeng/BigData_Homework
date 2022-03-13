import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.Media.print;


public class FlowCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Flow>{
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length < 10) {
                return;
            }
            String phone = fields[1];
            int up = Integer.parseInt(fields[8]);
            int down = Integer.parseInt(fields[9]);

            context.write(new Text(phone), new Flow(up, down));
            System.out.println("map key: " + phone);
        }

    }

    public static class IntSumReducer
            extends Reducer<Text,Flow,Text,Flow> {

        @Override
        public void reduce(Text key, Iterable<Flow> values, Context context
        ) throws IOException, InterruptedException {
            int up = 0;
            int down = 0;
            int total = 0;
            for (Flow flow : values) {
                up += flow.getUp();
                down += flow.getDown();
                total += flow.getTotal();
            }
            context.write(key, new Flow(up, down, total));
            System.out.println("reduce key: " + key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: FlowCount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Flow Count");
        job.setJarByClass(FlowCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
