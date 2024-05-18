import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private int recommendationsIndex = -1;
    private boolean headerProcessed = false;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        if (!headerProcessed) {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equalsIgnoreCase("recommendations")) {
                    recommendationsIndex = i;
                    break;
                }
            }
            headerProcessed = true;
            return; 
        }

        if (recommendationsIndex != -1 && columns.length > recommendationsIndex) {
            try {
                int recommendations = Integer.parseInt(columns[recommendationsIndex]);
                if (recommendations > 990) {
                    context.write(key, value);
                }
            } catch (NumberFormatException e) {
            }
        }
    }
}
