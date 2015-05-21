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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 矩阵第二次相乘
 * 
 * 离散数学中矩阵二次相乘后斜对角线上的值表示从该顶点出发经过两个顶点再回到该顶点的路径个数
 * 
 * @author gaoyang
 *
 */

public class MatrixLinkThreeEdge {

    public static class ThreeEdgeMap extends Mapper<Object, Text, Text, Text>{
    	
       
    	HashMap<String ,HashSet<String>> map = new HashMap<String ,HashSet<String>>();
  	
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
        	
        	String one = line[0].split("#")[0];
        	String three = line[0].split("#")[1];
        	
        	int num = Integer.parseInt(line[1]); 
        	
        	if(map.containsKey(three))
    		{
    			HashSet<String> set = map.get(three);       			
    			for(String str : set)
    			{
    				if(str.equals(one))
    				{
    					context.write(new Text("triangle"), new Text(num+""));
    				}
    			}       			
    		}
        	       	
       	
        }
    }
          
    
    public static class ThreeEdgeCombiner extends Reducer<Text, Text, Text, Text>{
        
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
           	
        	int sum = 0 ;
			
			for(Text value:values){
				sum += Integer.parseInt(value.toString());
			}
			context.write(key,new Text(sum+""));     	     	
        }
    	
    }
    
    
    public static class ThreeEdgeReduce extends Reducer<Text, Text, Text, Text> {          
             
			public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
				
				int sum = 0 ;
				
				for(Text value:values){
					sum += Integer.parseInt(value.toString());
				}
				context.write(new Text("total"),new Text(sum/6+""));                            
            }     		
    }
    

	public static void main(String[] args) throws Exception {
 	
		Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "TriangleCount-Step3");
    	
        job.setJarByClass(MatrixLinkThreeEdge.class);
           
                        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(ThreeEdgeMap.class);    
        job.setCombinerClass(ThreeEdgeCombiner.class);
        job.setReducerClass(ThreeEdgeReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
    }
    
} 

