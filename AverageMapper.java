import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text firstCharacter = new Text();

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

  
    String line = value.toString();

    for (String word : line.split("\\W+")) {
      if (word.length() > 0) {
    	  firstCharacter.set(word.substring(0,1));
        /*
         * Call the write method on the Context object to emit a key
         * and a value from the map method.
         */
      int length=word.length();
    	  
        context.write(new Text(firstCharacter), new IntWritable(length));
      }
    }
  }
}
