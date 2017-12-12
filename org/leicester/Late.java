/**

Author : Alexandru Marinus

The Map step of the program produces pairs of form
(airline_name,num_departures|","|delay_16_to_30mins|","|...delay_over_361_mins)
The Combiner reduces by the number of pairs
by checking for duplicates,  via the use of a HashMap.
The remaining pairs are then handled by the Reducer.
The Reducer produces the final pairs
(airline_name|","|year, P)
where P is greater than 50% and represents the percentage
of departures per airline in a certain year delayed over 31 minutes.

*/

package org.leicester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Late {

  public static class LateMapper
       extends Mapper<Object, Text, Text, Text>{

    /* Key and value for percentage computations */
    private Text delayKey = new Text(), delayValue = new Text();

	public static String[] splitbycomma(String S) {
            ArrayList<String> L = new ArrayList<String>();
            String[] a = new String[0];
            int i = 0;
            while (i<S.length()) {
                    int start = i;
                    int end=-1;
                    if (S.charAt(i)=='"') {
                            end = S.indexOf('"',i+1);
                    }
                    else {
                            end = S.indexOf(',',i)-1;
                            if (end<0) end = S.length()-1;
                    }
                    L.add(S.substring(start,end+1));
                    i = end+2;
            }
            return L.toArray(a);
    	}

   public void map(Object key, Text value,
	Context context) throws IOException, InterruptedException {

	/* trim() is called to remove trailing
	whitespaces from field values */
	String fileLine = value.toString();
	if (fileLine.isEmpty())
		return;

	// The line with field names is ignored
	String[] tokens = splitbycomma(fileLine);
	if (tokens[0].equals("run_date"))
		return;

	String year = tokens[1].trim().substring(0,4);

	/* Lines with charter entries or without matched
	flights are ignored */
	String airlineName = tokens[5].trim();
	String arrivalDeparture = tokens[6].trim();
	if (!arrivalDeparture.equals("D"))
		return;

	String scheduledCharter = tokens[7].trim();
	if (scheduledCharter.equals("C"))
		return;

	String numFlightsMatched = tokens[8].trim();
	if (numFlightsMatched.equals("0"))
		return;

	String delay31To60MinsPercent = tokens[12].trim();
	String delay61To180MinsPercent = tokens[13].trim();
	String delay181To360MinsPercent = tokens[14].trim();
	String delayOver361MinsPercent = tokens[15].trim();

	// A (key, value) pair is formed from the fields
	delayKey.set(airlineName+","+year);
	delayValue.set(numFlightsMatched+","+delay31To60MinsPercent+
		","+delay61To180MinsPercent+","+delay181To360MinsPercent+
		","+delayOver361MinsPercent);

	// The Mapper writes the pair
	context.write(delayKey, delayValue);
    }
  }

  public static class LateCombiner extends Reducer<Text, Text, Text, Text> {

	/* A HashMap is used to store the keys and their non-repeating
	values (to ignore the duplicates emitted) */
	private HashMap<Text, HashSet<Text>> airlineToDelayMap = new HashMap<Text, HashSet<Text>>();

	public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {

		airlineToDelayMap.put(key, new HashSet<Text>());
		for (Text value : values)
			/* If the value is not is the HashMap's entry
			for the key, it is added, or else it is skipped */
			if (!airlineToDelayMap.get(key).contains(value)) {
				airlineToDelayMap.get(key).add(value);
				// The Combiner writes the pair
				context.write(key, value);
			}
	}
  }

  public static class LateReducer extends Reducer<Text,Text,Text,Text> {

	private Text result = new Text();

	 public static String[] splitbycomma(String S) {
            ArrayList<String> L = new ArrayList<String>();
            String[] a = new String[0];
            int i = 0;
            while (i<S.length()) {
                    int start = i;
                    int end=-1;
                    if (S.charAt(i)=='"') {
                            end = S.indexOf('"',i+1);
                    }
                    else {
                            end = S.indexOf(',',i)-1;
                            if (end<0) end = S.length()-1;
                    }
                    L.add(S.substring(start,end+1));
                    i = end+2;
            }
            return L.toArray(a);
	}

    public void reduce(Text key, Iterable<Text> values,
	Context context) throws IOException, InterruptedException {

	int index;
	int numDepartures = 0, numTotalDelayedDepartures = 0, numTotalDepartures = 0;
	String[] valueTokens = null;
	Double relativePercent, absolutePercent;

	/* The absolute departure delay percentage
	is computed for the key */
	for (Text value: values) {
		relativePercent = 0.0;
		valueTokens = splitbycomma(value.toString());
		numDepartures = Integer.parseInt(valueTokens[0]);
		numTotalDepartures += numDepartures;
		for (index = 1; index <= 4; index ++)
			relativePercent += Double.parseDouble(valueTokens[index]);
		numTotalDelayedDepartures += (int) Math.floor((numDepartures * relativePercent) / 100);
	}

	absolutePercent = numTotalDelayedDepartures * 100.0 / numTotalDepartures;
	/* If it exceeds 50%, then the Reducer
	emits the (key, value) pair */
	if (absolutePercent > 50) {
		result.set(absolutePercent.toString());
		context.write(key, result);
	}
    }
  }

  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Late");
	job.setJarByClass(Late.class);
	job.setMapperClass(LateMapper.class);
	job.setCombinerClass(LateCombiner.class);
	job.setReducerClass(LateReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
