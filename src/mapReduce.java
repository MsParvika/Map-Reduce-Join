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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class mapReduce {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration c = new Configuration();
        Job job = new Job(c, "mapReduce");
        job.setJarByClass(mapReduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Object.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        private Text text = new Text();

        private DoubleWritable doubleWritable = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tuplesItr = new StringTokenizer(value.toString(), "\n");
            while (tuplesItr.hasMoreTokens()) {
                String tuple = tuplesItr.nextToken();
                StringTokenizer stringTokenizer = new StringTokenizer(tuple, ", ");
                String tableName = stringTokenizer.nextToken();
                String id = stringTokenizer.nextToken();
                doubleWritable.set(Double.parseDouble(id));
                text.set(tuple);
                context.write(doubleWritable, text);
            }
        }
    }

    public static class Reduce extends Reducer<DoubleWritable, Text, Object, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> tuples = new ArrayList<String>();
            List<String> table1Tuples = new ArrayList<String>();
            List<String> table2Tuples = new ArrayList<String>();
            Text text = new Text();
            StringBuilder stringBuilder = new StringBuilder();
            String table1 = null;
            for (Text value : values) {
                tuples.add(value.toString());
            }
            if(tuples.size() < 2) {
                return;
            } else {
                table1 = tuples.get(0).split(", ")[0];
                for (String tuple : tuples) {
                    if (table1.equals(tuple.split(", ")[0])) {
                        table1Tuples.add(tuple);
                    } else {
                        table2Tuples.add(tuple);
                    }
                }
                if(table1Tuples.size() == 0 || table2Tuples.size() == 0) {
                    return;
                }else {
                    for (String table1Tuple : table1Tuples) {
                        for (String table2Tuple : table2Tuples) {
                            stringBuilder.append(table1Tuple).append(", ").append(table2Tuple).append("\n");
                        }
                    }
                    text.set(stringBuilder.toString().trim());
                    context.write(null, text);
                }
            }
        }
    }
}
