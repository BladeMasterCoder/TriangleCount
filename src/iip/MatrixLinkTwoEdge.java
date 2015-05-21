package iip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 矩阵第一次相乘
 * 离散数学中矩阵一次相乘后斜对角线上的值表示从该顶点出发经过一个顶点再回到该顶点的路径个数
 * 
 * @author gaoyang
 *
 */

public class MatrixLinkTwoEdge {

    public static class TwoEdgeMap extends Mapper<Object, Text, Text, Text>{
       
    	HashMap<String ,HashSet<String>> map = new HashMap<String ,HashSet<String>>();
  	
    	
    	
    	/**
    	 * 初始化map，map中存储的是矩阵的邻接表，由此邻接表才能计算矩阵相乘
    	 */
    	
    	protected void setup(Context context) throws IOException, InterruptedException {
 		
    		Configuration conf = context.getConfiguration();    	    
    		Path path = new Path("hdfs://master01:54310/user/2015st12/output5/part-r-00000");
    		
    		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master01:54310/user/2015st12/output5/part-r-00000"), conf);   		
    		FSDataInputStream in = hdfs.open(path);
    		
    		BufferedReader buff = new BufferedReader(new InputStreamReader(in));
   		
    		String str = null;   	
    		
    		while((str = buff.readLine()) != null){
    			
        		String[] line = str.split("\t");       		      		
        		String start = line[0];
        		
        		String []appends = line[1].split(",");    
        		
        		if(map.containsKey(start))
        		{
        			for(String val:appends)
        			{
        				map.get(start).add(val);
        			}
        		}
        		else
        		{
        			HashSet<String> set = new HashSet<String>();
        			for(String val:appends)
        			{
        				set.add(val);
        			}
        			map.put(start, set);
        		}  
    		}
   		
    		buff.close();
    		in.close();      	
        } 
    	
    	
        public void map(Object key,Text value,Context context)
        		throws IOException,InterruptedException {
        	
        	String[] line = value.toString().split("\t");
        	
        	String start = line[0];
        	
        	String []points = line[1].split(","); 
        	       	
        	for(String point:points)
        	{
        		if(map.containsKey(point))
        		{
        			HashSet<String> set = map.get(point);       			
        			for(String str : set)
        			{
        				if(!str.equals(start))
        				{
        					context.write(new Text(start+"#"+str), new Text(1+""));
        				}
        			}       			
        		}
        	}
        	
        	
        }
    }
          
    
    public static class TwoEdgePartition extends Partitioner<Text,Text>{

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			
			int maxNum = 400000000;
			
			int bound = maxNum / numPartitions + 1;
			
			
			int keyNum = Integer.parseInt(key.toString().split("#")[0]);
			
			
			for(int i = 0 ; i < numPartitions ; i ++){
				
				if(keyNum >= bound * (numPartitions-1)){
					return (numPartitions-1);
				}
				
				if(keyNum < bound * (i+1) && keyNum >= bound * i){
					return i;
				}
				
			}		
			return -1;
		}
    	
    	
    }
    
    public static class TwoEdgeReduce extends Reducer<Text, Text, Text, Text> {
    	
           
             
			public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
				
				int sum = 0 ;
				
				for(Text value:values){
					sum += Integer.parseInt(value.toString());
				}
				context.write(key,new Text(sum+""));
                             
            }     
		
    }
    
    
    
    

	public static void main(String[] args) throws Exception {
 	
		Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "TriangleCount-Step2");
   
        job.setJarByClass(MatrixLinkTwoEdge.class);
            
                        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(TwoEdgeMap.class);  
        job.setCombinerClass(TwoEdgeReduce.class);
        job.setPartitionerClass(TwoEdgePartition.class);
        job.setReducerClass(TwoEdgeReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
    }
    
} 
