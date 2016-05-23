import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.*;


/* This program generates the top 10 friend recommendations for every person in the social network data based
 * on their mutual friends(only second degree).
 * The assumption here is that friendship is two-way i.e x is friend with y then y is friend with x.
 */
public class GenerateRecommendation {

/* The map method emits key value pairs for first degree and second degree connections 
 * Eg : if x and y are direct friends then key value pair generated is (x,(1,y)) and (y,(1,x)) 
   and if x and y are second degree friends then key value pair generated is(x,(2,y)) and (y,(2,x))
   */
	
	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String Inputline = value.toString();
			String[] userFriends = Inputline.split("\t"); // splitting the input to get the person and friend ID's seperately
															
			if (userFriends.length == 2) {
				String user = userFriends[0];
				LongWritable ukey = new LongWritable(Long.parseLong(user));
				String[] friends = userFriends[1].split(","); // Storing each friend seperately
															

				LongWritable friendfirstKey = new LongWritable();
				Text friendfirstValue = new Text();

				LongWritable friendsecondKey = new LongWritable();
				Text friendsecondValue = new Text();
				Text friendinitValue = new Text();
				for (int i = 0; i < friends.length; i++) { 
					friendfirstValue.set("1," + friends[i]); //Constructing key for first degree friend keys
					friendinitValue.set("1," + ukey);
					context.write(ukey, friendfirstValue);
					context.write(new LongWritable(Long.parseLong(friends[i])),
							friendinitValue);
					friendfirstKey.set(Long.parseLong(friends[i]));
					friendfirstValue.set("2," + friends[i]);
					for (int j = i + 1; j < friends.length; j++) {
						friendsecondKey.set(Long.parseLong(friends[j]));  //Constructing key for second degree friend keys
						friendsecondValue.set("2," + friends[j]);
						context.write(friendfirstKey, friendsecondValue);
						context.write(friendsecondKey, friendfirstValue);
					}
					friendfirstValue.clear();
					friendsecondValue.clear();
					friendinitValue.clear();

				}

			}
		}

	}

	/*The reducer method receives the key value pairs emitted by the mapper and keys which are same reach the same reducer*/
	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> outputFrmMapper,
				Context context) throws IOException, InterruptedException {
			ArrayList<String> val = new ArrayList<String>();
			for (Text v : outputFrmMapper) {
				val.add(v.toString());
			}
			String[] value;
			HashMap<String, Long> hashMapper = new HashMap<String, Long>();
			for (String txt : val) {         
				value = (txt.toString()).split(",");
				if (value[0].equals("1"))   
					hashMapper.put(value[1], (long) -1);  // if they are direct friends then not including in count and storing -1(because they need no recommendation)
				else if (value[0].equals("2")) {
					if (hashMapper.containsKey(value[1])) {
						if (hashMapper.get(value[1]) != -1)
							hashMapper.put(value[1],
									(long) (hashMapper.get(value[1]) + 1)); //Summing up the mutual friends
					} else
						hashMapper.put(value[1], (long) 1);
				}

			}

			ArrayList<Entry<String, Long>> frnlist = new ArrayList<Entry<String, Long>>();

			for (Entry<String, Long> entry : hashMapper.entrySet()) { //Eliminating the recommendation if they are already friends
				if (entry.getValue() != -1)
					frnlist.add(entry);
			}

			// Sorting the mutual friend count to get top 10
			Collections.sort(frnlist, new Comparator<Entry<String, Long>>() {
				public int compare(Entry<String, Long> e1,
						Entry<String, Long> e2) {
					if (e2.getValue().compareTo(e1.getValue()) == 0) {
						return (e1.getKey().compareTo(e2.getKey()));
					}
					return (e2.getValue().compareTo(e1.getValue()));
				}
			});
             
			/* Printing the top 10 recommendations */
			ArrayList<String> topRecom = new ArrayList<String>();
			int Max_value = 10;
			for (int i = 0; i < Math.min(Max_value, frnlist.size()); i++) {
				topRecom.add(frnlist.get(i).getKey());
			}
			String listStringtoOutput = "";
			for (String s : topRecom) {
				listStringtoOutput += s + ",";
			}
			Text recFriends = new Text();
			recFriends.set(listStringtoOutput);
			context.write(key, recFriends);
			recFriends.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "FriendRec");
        //Set mapper and reducer class 
		job.setJarByClass(GenerateRecommendation.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
        //Set mapper and reducer key/value types
		job.setOutputKeyClass(LongWritable.class);

		job.setOutputValueClass(Text.class);
        //Set Input Path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//Set Output Path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

