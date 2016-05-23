import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.InputStreamReader;

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/*This program retrieves zipcode and names of the mutual friends of two given users, the
 * name and zipcode details are stored in a different file and here the concept of in-memory join
 * is illustrated.
 */
public class GetAddressDetailsOfMutualFriends {

	// Mapper Class
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		HashMap<String, String> myMap;

		@Override
		/* The userdetails file is loaded in-memory and stored in a global
		   hashmap in the below method and used later in the map method */
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method
			super.setup(context);
			// read data to memory on the mapper.
			myMap = new HashMap<String, String>();
			Configuration conf = context.getConfiguration();

			Path part = new Path(
					"hdfs://localhost:54310/user/supriya/userdata.txt");//Location

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] arr = line.split(",");
					if (arr.length > 1) {
						myMap.put(arr[0].trim(), line);
					}
					line = br.readLine();
				}
			}

		}

		// The mapper method emits the friend ID's of user 1 and user 2 along with their address
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
						|| param2.equals(userWithMutualFriends[0])) {
					Text friend1Value = new Text();

					for (int i = 0; i < friends.length; i++) {
						friend1Value.set(param1 + " " + param2);
						if (!param1.equals(friends[i])
								&& !param2.equals(friends[i])) {  //Check if a particular user is friends with both user1 and user2
							if (myMap.containsKey(friends[i])) {
								String valueFromMap = friends[i] + ":"
										+ myMap.get(friends[i]);
								context.write(friend1Value, new Text(
										valueFromMap));

							}
						}

					}

					friend1Value.clear();

				}

			}
		}
	}

	/* The reducer will receive friend ID's of both users and then reducer will
	   output only the mutual friends along with their name and zipcode */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> outputFromMap,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friends = new HashMap<String, Integer>();
			HashMap<String, String> friendsWithAddress = new HashMap<String, String>();

			for (Text val : outputFromMap) {

				if (friends.containsKey(val.toString().split(":")[0])) {
					Integer count = friends.get(val.toString().split(":")[0]);
					friends.put(val.toString().split(":")[0], count + 1);
				} else {
					friends.put(val.toString().split(":")[0], 1);
					friendsWithAddress.put(val.toString().split(":")[0], val
							.toString().split(":")[1]);
				}

			}

			String valueFromReducer = new String();
			valueFromReducer = "";
			for (Entry<String, Integer> entry : friends.entrySet()) {
				if (entry.getValue() > 1) {
					valueFromReducer = valueFromReducer
							+ friendsWithAddress.get(entry.getKey()).split(",")[1]
							+ ":"
							+ friendsWithAddress.get(entry.getKey()).split(",")[6]
							+ ", ";

				}
			}
			if (valueFromReducer != "") {
				valueFromReducer = "[" + valueFromReducer + "]";
				context.write(key, new Text(valueFromReducer));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("User1", args[3]);
		conf.set("User2", args[4]);

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "FriendRecommendation");

		job.setJarByClass(GetAddressDetailsOfMutualFriends.class);
		//Setting up the mapper and reducer classes
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//Setting up the key/value type of mapper and reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//Setting up the input path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//Setting up the output path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
