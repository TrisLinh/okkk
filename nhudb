import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ElectricityConsumptionMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        String year = columns[0];
        int consumption = Integer.parseInt(columns[2]);
        context.write(new Text(year), new IntWritable(consumption));
    }
}

public class ElectricityConsumptionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        double average = (double) sum / count;
        if (average > 30) {
            context.write(key, new IntWritable(1));
        }
    }
}
