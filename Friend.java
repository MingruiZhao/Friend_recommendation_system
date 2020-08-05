
package hadoop;
 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
 
public class Friend {
 
	
	static class info{
		private static String lis;
		private static int siz;
		public info(int siz, String lis){
			this.siz = siz;
			this.lis = lis;
		}
		public static int getSize(){
			return siz;
		}
		public void setSize(int siz){
			this.siz = siz;
		}
		public static String getList(){
			return lis;
		}
		public void setList(String lis){
			this.lis = lis;
		}
	}
	static class ValuePair implements WritableComparable<ValuePair> {
		 
		private String is_friend;
		private String relation;
 
		public String getFriend() {
			return is_friend;
		}
 
		public void setFriend(String is_friend) {
			this.is_friend = is_friend;
		}
 
		public String getRelate() {
			return relation;
		}
 
		public void setUnion(String relation) {
			this.relation = relation;
		}
 
		public ValuePair() {
			// TODO Auto-generated constructor stub
		}
 
		public ValuePair(String n, String u) {
			this.is_friend = n;
			this.relation = u;
		}
 
		@Override
		public void readFields(DataInput in) throws IOException {
			this.is_friend = in.readUTF();
			this.relation = in.readUTF();
		}
 
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.is_friend);
			out.writeUTF(this.relation);
		}
 
		@Override
		public int compareTo(ValuePair o) {
			if (this.is_friend.equals(o.getFriend())) {
				return this.relation.compareTo(o.getRelate());
			} else {
				return this.is_friend.compareTo(o.getFriend());
			}
		}
		
		
 
	}
	static class Map extends Mapper<LongWritable, Text, IntWritable, ValuePair> {
 
		private IntWritable outkey = new IntWritable();
		private ValuePair value = new ValuePair();
 
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = value.toString();
			//key
			String[] sz = input.split("	");		
			this.outkey.set(Integer.parseInt(sz[0]));
			//value
			String[] sz1 = null;
			if(sz.length > 1){
				sz1 = sz[1].split(",");
				//they are is_friend set value
				for (int i = 0; i < sz1.length; i++) {
					this.value.setFriend(sz1[i]);
					this.value.setUnion("yes");
					context.write(this.outkey, this.value);
				}
				
				for (int i = 0; i < sz1.length; i++) {
					for (int j = i + 1; j < sz1.length; j++) {
						// 1	2,3,4,5,6
						//out key = 2, set is_friend = 3
						//relation 2: (3,1) 
						this.outkey.set(Integer.parseInt(sz1[i]));
						this.value.setFriend(sz1[j]);
						this.value.setUnion(sz[0]);
						context.write(this.outkey, this.value);
						
						this.outkey.set(Integer.parseInt(sz1[j]));
						this.value.setFriend(sz1[i]);
						context.write(this.outkey, this.value);
					}
				}
			}else{
				this.value.setFriend("0");
				this.value.setUnion("no");
				context.write(this.outkey, this.value);
			}
		}
	}
	static class Reduce extends Reducer<IntWritable, ValuePair, IntWritable, Text> {
 
		private IntWritable outKey = new IntWritable();
		private Text outValue = new Text();
		
		@Override
		protected void reduce(IntWritable key, Iterable<ValuePair> values,
				Context context) throws IOException, InterruptedException {
			this.outKey = key;
			Multimap<String, String> map = HashMultimap.create();
			List<Integer> size = new ArrayList<Integer>();
			List<String> list = new ArrayList<String>();
			List<String> result = new ArrayList<String>();
			ArrayList<info> d = new ArrayList<>();
			int counter = 0;
			for (ValuePair v : values) {
				//get all value regarding to key
				map.put(v.getFriend(), v.getRelate()); 
//				String tmp = v.getFriend() + " " + v.getRelate();				
//				this.outValue.set(tmp);
//				context.write(this.outKey, this.outValue);
			}
			// store the output
			StringBuilder outString = new StringBuilder();
			//get all value regards to the key
			Set<String> keys = map.keySet();;
			counter = 0;
			for(String s : keys){
				StringBuilder outStr = new StringBuilder();
				boolean stat = true;
				Collection<String> v = map.get(s);
				outStr.append(s);
				String tmp = new String();
				//for(int i = 0; i< ((CharSequence) v).length();i++){
				//System.out.print("size: "+v.size()+"  end.  ");
				//System.out.print(((CharSequence) v).length());
				for(String u : v){
					if(u.equals("yes")){
						stat = false;
						break;
					}else if(u.equals("no")){
						stat = false;
						break;
					}else{
						tmp += u + ", ";
					}
				}
				if(tmp.length()>2){
					tmp = tmp.substring(0, tmp.length()-2);
				}
				if(stat){
					list.add(s);
					size.add(v.size());
//					System.out.print(d.getSize());
					outString.append(outStr);
				}
			
			}
			for(int j = 0; j < size.size()-1;j++){
				for(int i = 0; i < size.size()-1;i++){
						if(size.get(i) < size.get(i+1)){
							String temp = list.get(i+1);
							int t = size.get(i+1);
							
							size.set(i+1,size.get(i));
							size.set(i,t);
							list.set(i+1,list.get(i));
							list.set(i,temp);
						}
				}
			}

			if(list.size() >= 10){
				for(int i = 0; i < 10;i++){
					result.add(i, list.get(i));
				}
			}else{
				for(int i = 0; i < list.size();i++){
					result.add(i, list.get(i));
				}
			}
			outValue.set(result.toString());
			context.write(outKey, outValue);
			//System.out.print(info.getSize());
			
		}	
	}
	private static String inputPath = "in-is_friend";
	private static String outputPath = "out-is_friend";
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "is_friend recomment");
		job.setJarByClass(Friend.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ValuePair.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public Entry<Integer, String> entry(int size, String s) {
		// TODO Auto-generated method stub
		return null;
	}
 
}