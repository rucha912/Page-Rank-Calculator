package MapReduce.Hadoop;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class pageRank extends Configured implements Tool {
	private static final String N = "n";
	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner.run( new pageRank(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {
		FileSystem fs = FileSystem.get(getConf());
		Job jobCount  = Job.getInstance(getConf(), "pagerank"); //create a job to count the number of nodes
		jobCount.setJarByClass( this.getClass());
		long n_val;
		
		// Job to calculate the value of N
		FileInputFormat.addInputPaths(jobCount,  args[0]);
		FileOutputFormat.setOutputPath(jobCount,  new Path(args[3]));
		jobCount.setMapperClass( mapCount.class);
		jobCount.setReducerClass( reduceCount.class);
		jobCount.setMapOutputValueClass(IntWritable.class);
		jobCount.setOutputKeyClass( Text.class);
		jobCount.setOutputValueClass(IntWritable.class);
		jobCount.waitForCompletion(true);
		fs.delete(new Path(args[3]), true);
		
		// n value is calculated
		n_val = jobCount.getCounters().findCounter("Result", "Result").getValue();

		// Job to get the title and outlinks.
		Job job1  = Job.getInstance(getConf(), "pagerank");
		job1.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[1]+"job0"));
		job1.getConfiguration().setStrings(N, n_val + "");
		job1.setMapperClass( Map1.class);
		job1.setReducerClass( Reduce1.class);
		job1.setOutputKeyClass( Text.class);
		job1.setOutputValueClass( Text.class);
		job1.waitForCompletion(true);

		int i = 1;
		//Job to calculate the page page_rank and iterate for 10 iterations. We assume that the matrix will converge after 10 iterations   
		for(i=1;i<=10;i++){
			Job job2  = Job.getInstance(getConf(), "pagerank");
			job2.setJarByClass( this.getClass());
			FileInputFormat.addInputPaths(job2,  args[1]+"job"+(i-1));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"job"+i));    
			job2.setMapperClass( Map2.class);
			job2.setReducerClass( Reduce2.class);
			job2.setOutputKeyClass( Text.class);
			job2.setOutputValueClass( Text.class);
			job2.waitForCompletion(true);
			fs.delete(new Path(args[1]+"job"+(i-1)), true); 
		}
		
		//Job to sort the values in descending order.
		Job job3  = Job.getInstance(getConf(), "pagerank");
		job3.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(job3,  args[1]+"job"+(i-1));
		FileOutputFormat.setOutputPath(job3,  new Path(args[2]));
		job3.setMapperClass( Map3.class);
		job3.setReducerClass( Reduce3.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass( Text.class);
		job3.setOutputValueClass( DoubleWritable.class);
		job3.waitForCompletion(true);
		return 1;

	}
	public static class mapCount extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern title = Pattern
				.compile("<title>(.*?)</title>"); //number of nodes corresponds to these lines in input

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			if (line != null && !line.isEmpty()) {
				Text currentUrl = new Text();

				Matcher matcher1 = title.matcher(line);

				if (matcher1.find()) {
					currentUrl = new Text(matcher1.group(1));
					context.write(new Text(currentUrl), new IntWritable(1));
				}

			}

		}
	}

	public static class reduceCount extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		int counter = 0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException,
				InterruptedException {
			this.counter++;                 //keep incrementing counter to get value of n
		}

		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			context.getCounter("Result", "Result").increment(counter);
		}
	}



	public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
 
		private static final Pattern title = Pattern.compile("<title>(.*?)</title>");        //find the title of the page
		private static final Pattern text = Pattern.compile("<text+\\s*[^>]*>(.*?)</text>"); //find the text on the page
		private static final Pattern outlink = Pattern.compile("\\[\\[(.*?)\\]\\]");            //find the outlinks on the page
		double n;

		public void initial(Context context) throws IOException, InterruptedException{
			n = context.getConfiguration().getDouble(N, 1);
		}


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			if(line != null && !line.isEmpty() ){
				Text url  = new Text();
				Text currlinks  = new Text();
				String links = null;
				Matcher matcher1 = title.matcher(line);
				Matcher matcher2 = text.matcher(line);
				Matcher matcher3 = null;

				if(matcher1.find()){
					url  = new Text(matcher1.group(1));
				}

				if(matcher2.find()){
					links = matcher2.group(1);
					matcher3 = outlink.matcher(links);
				}
				double onebyn = (double)1/(n);
				StringBuilder str = new StringBuilder("##"+onebyn+"##");
				int count=1;
				while(matcher3 != null && matcher3.find()) {
					links=matcher3.group(1);
					if(count>1)
						str.append("@!#"+matcher3.group(1));       
					else if(count==1){
						str.append(links);
						count++;
						}
				}
				currlinks = new Text(str.toString());
				context.write(url,currlinks);
			}        
		}
	}

	public static class Reduce1 extends Reducer<Text ,  Text ,  Text ,  Text > {
		public void reduce( Text word,  Text counts,  Context context)
				throws IOException,  InterruptedException {
			context.write(word,counts);
		}
	}

	public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {   //mapper for calculating page ranks
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			float page_rank=0.0f;                              //set the rank to 0 initially
			String line  = lineText.toString(),srank="";       
			String[] vals=line.split("##");	            	                 
			float initialrank =Float.parseFloat(vals[1]);
			if(vals.length < 3){
				context.write(new Text(vals[0]),new Text("##"+vals[1])); 
			}
			else if(vals.length==3){
				String[] olinks=vals[2].split("@!#");
				context.write(new Text(vals[0]),new Text("##"+vals[1]+"##"+vals[2]));
				for(int j=0;j< olinks.length;j++){	
					String pages= olinks[j];             //calculate the ranks for outlinks
					page_rank=0.85f * (initialrank/(float)(olinks.length)) ;  //d value is 0.85
					srank=Float.toString(page_rank);
					pages=pages.replace(" ",""); // replacing the empty space
					context.write(new Text(pages+"!*"), new Text("##"+srank)); //Add newly calculated pagerank
				}
			}
		}
	}
	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text > {
		static String var="";
		static String key_val="";
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {			        
			String key=word.toString(),rankstr="";
			String[] arr;
			float page_rank=0.0f;
			key=key.trim(); // trimming the key 
			
			if(!key.contains("!*")){
				if(!key_val.equals(""))       //if key is present
				{
					context.write(new Text(key_val), new Text(var));}
				for(Text value:counts){
					rankstr=value.toString();
				}
				
				key_val=key;  //storing key and var for further iterations
				var=rankstr;
			}
			else{ 											
				key=key.substring(0,key.length()-2); //storing the key
				key.trim();

				for(Text value:counts){ 
					rankstr=value.toString();
					rankstr=rankstr.substring(2);
					page_rank+=Float.parseFloat(rankstr);			        		  
				}
				page_rank+=0.15f;

				if(key.equals(key_val)){
					arr=var.split("##");
					if(arr.length>2){
						var="##"+page_rank+"##"+arr[2]; //
					}
					else{
						var="##"+page_rank; //if the page does not have any outlinks
					}
					key_val="";
					context.write(new Text(key),new Text(var));
				}
				else{
					if(!key_val.equals("")){ 
						context.write(new Text(key_val), new Text(var) ); 
						key_val="";
					}
					context.write(new Text(key), new Text("##"+ Float.toString(page_rank) ) );
				}
			}
		}
	}


	public static class Map3 extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {  //mapper to sort the final pagerank values
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {  
			String line  = lineText.toString();
			String[] array=line.split("##");
			double page_rank= Double.parseDouble(array[1]);
			context.write(new DoubleWritable(-1 * page_rank), new Text(array[0])); // multiplying by -1 so the values are sorted in descending order
		}
	}
	public static class Reduce3 extends
	Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable counts, Iterable<Text> word,
				Context context) throws IOException, InterruptedException {
			double var = 0;		
			var = counts.get() * -1; // restoring values to original values by multiplying by -1
			String s = "";
			for (Text w : word) {
				s = w.toString();
				context.write(new Text(s), new DoubleWritable(var));
			}
		}
	}
}
