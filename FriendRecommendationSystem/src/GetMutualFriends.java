import java.io.IOException;
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


/* This program determines all the mutual friends of two given input users*/

public class GetMutualFriends {

	// Mapper Class
	//The mapper method emits the friend ID's of user 1 and user 2
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			String param1 = conf.get("User1");
			String param2 = conf.get("User2");

			String line = value.toString();
			String[] userWithMutualFriends = line.split("\t");
			if (userWithMutualFriends.length == 2) {
				String[] friends = userWithMutualFriends[1].split(",");
				if (param1.equals(userWithMutualFriends[0])
						|| param2.equals(userWithMutualFriends[0])) { //Check for person ID's that are friends with user1 and user2
					Text friend1Value = new Text();

					for (int i = 0; i < friends.length; i++) {
						friend1Value.set(param1 + " " + param2);
						if (!param1.equals(friends[i])
								&& !param2.equals(friends[i]))
							context.write(friend1Value, new Text(friends[i]));
					}
					friend1Value.clear();

				}

			}
		}

	}

	//The reducer method will receive friend ID's of both users and then reducer will output only the mutual friends
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> outputFromMap,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friends = new HashMap<String, Integer>();

			for (Text val : outputFromMap) {    //Storing all the recieved ID's in a hashmap 

				if (friends.containsKey(val.toString()))
					friends.put(val.toString(), friends.get(val.toString()) + 1);
				else
					friends.put(val.toString(), 1);
			}

			String valueFromReducer = new String();   //Checking if any friend ID was received twice then such ID's are mutual friend ID's
			for (Entry<String, Integer> entry : friends.entrySet()) {
				if (entry.getValue() > 1) {
					valueFromReducer = valueFromReducer + entry.getKey() + ",";
				}
			}

			context.write(key, new Text(valueFromReducer));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("User1", args[2]);
		conf.set("User2", args[3]);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "FriendRecommendation");
		//Set mapper and reducer class 
		job.setJarByClass(GetMutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//Set mapper and reducer key/value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//Set Input Path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//Set Output Path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}