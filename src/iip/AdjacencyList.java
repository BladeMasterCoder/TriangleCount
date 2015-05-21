package iip;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 这个类的作用是得到图的邻接表
 * 
 * @author gaoyang
 *
 */


public class AdjacencyList {

    public static class AdjacencyListMap extends Mapper<Object, Text, Text, Text>{
    	
    	HashSet<String> set = new HashSet<String>(); //记录已经发射的键值对，防止重复
       
    	
    	/**
    	 * value 示例输入 :  100 200
    	 * 其中数字代表顶点 一行表示顶点100 到顶点200有一条边
    	 * 
    	 * map 函数 对每一条边发射两条键值对 <100,200> 和<200,100>
    	 */
    	
        public void map(Object key,Text value,Context context)
        		throws IOException,InterruptedException {
        	
        	String[] points = value.toString().split(" ");
        	
        	
        	
        	if(!points[0].equals(points[1]))		// 如果不是顶点自身到自身的边
        	{
        		if(!set.contains(points[0]+"#"+points[1]))
        		{
        			set.add(points[0]+"#"+points[1]);
        			context.write(new Text(points[0]),new Text(points[1]));
            	}  
        		
        		if(!set.contains(points[1]+"#"+points[0]))
        		{
        			set.add(points[1]+"#"+points[0]);
        			context.write(new Text(points[1]),new Text(points[0]));
            	} 
        	}
        	
        }
    }
          
    
    public static class AdjacencyListReduce extends Reducer<Text, Text, Text, Text> {
    	
            private Text result = new Text();
             
            /**
             * 统计与每一个顶点相邻的所有边
             */
            
			public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
				
                StringBuilder out = new StringBuilder();
									
                for(Text value:values)
                {   
                	out.append(",");
                	out.append(value.toString());              	
                }                
                out.deleteCharAt(0);
                
                result.set(out.toString());                    
                context.write(key, result);               
            }     
		
    }
    
    
    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "TriangleCount-Step1");
        
        job.setJarByClass(AdjacencyList.class);
            
                        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(AdjacencyListMap.class);        
        job.setReducerClass(AdjacencyListReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
    }
    
}  
