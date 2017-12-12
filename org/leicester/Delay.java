/**

Author : Alexandru Marinus

The Map step of the program produces pairs of form
(airport_name, arrival_departure|","|num_flights_matched|"'"|avg_delay)
so that the distinction between arrival and departure
flights within the entries can be made.
The Combiner reduces the number of pairs
by checking for duplicates,  via the use of a HashMap.
The remaining pairs are then handled by the Reducer, which 
produces the final pairs
(airport_name, average_arrival_delay|","|average_departure_delay)
by calculating the mean of the time values.

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

public class Delay {

  public static class DelayMapper
       extends Mapper<Object, Text, Text, Text>{

    /* Key and value for delay computations */
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

	String[] tokens = splitbycomma(fileLine);

	// The line with field names is ignored
	if (tokens[0].equals("run_date"))
		return;

	String airportName = tokens[2].trim();
	String arrivalDeparture = tokens[6].trim();

	/* Lines with charter entries or without matched
	flights are ignored */
	String scheduledCharter = tokens[7].trim();
	if (scheduledCharter.equals("C"))
		return;
	String numFlightsMatched = tokens[8].trim();
	if (numFlightsMatched.equals("0"))
		return;

	String averageDelay = tokens[16].trim();

	// A (key, value) pair is formed from the fields
	delayKey.set(airportName);
	delayValue.set(arrivalDeparture+","+numFlightsMatched+","+averageDelay);

	// The Mapper writes the pair
	context.write(delayKey, delayValue);
    }

  }

  public static class DelayCombiner extends Reducer<Text, Text, Text, Text> {

	/* A HashMap is used to store the keys and their non-repeating
	values (to ignore the duplicates emitted) */
	private HashMap<Text, HashSet<Text>> airportToDelayMap = new HashMap<Text, HashSet<Text>>();

	public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {

		airportToDelayMap.put(key, new HashSet<Text>());
		for (Text value : values)
			/* If the value is not is the HashMap's entry
			for the key, it is added, or else it is skipped */
			if (!airportToDelayMap.get(key).contains(value)) {
				airportToDelayMap.get(key).add(value);
				// The Combiner writes the pair
				context.write(key, value);
			}
	}
  }

  public static class DelayReducer extends Reducer<Text,Text,Text,Text> {

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

	int numTotalArrivals = 0, numTotalDepartures = 0;
	double totalArrivalTime = 0, totalDepartureTime = 0;

	// The total delays are computed
	for (Text value: values) {
		String[] valueTokens = splitbycomma(value.toString());
		int numFlights = Integer.parseInt(valueTokens[1]);
		String arrivalDeparture = valueTokens[0];
		double delay = Double.parseDouble(valueTokens[2]);
		if (arrivalDeparture.equals("A")) {
			numTotalArrivals += numFlights;
			totalArrivalTime += numFlights * delay;
		}
		else {
			numTotalDepartures += numFlights;
			totalDepartureTime += numFlights * delay;
		}
	}

	// The average delays are computed and merged in the result
	totalArrivalTime /= numTotalArrivals;
	totalDepartureTime /= numTotalDepartures;
	result.set(totalArrivalTime + "," + totalDepartureTime);
	// The Reducer emits the (key, result) pair
	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Delay");
	job.setJarByClass(Delay.class);
	job.setMapperClass(DelayMapper.class);
	job.setCombinerClass(DelayCombiner.class);
	job.setReducerClass(DelayReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
