package iip;

public class Driver {
	
	
	public static void main(String[] args) throws Exception {
	
		String base = "hdfs://master01:54310/user/2015st12/";
	
		String [] path0 = {args[0],base+"output1"};
		AdjacencyList.main(path0);
		
		
		String [] path1 = {base+"output1",base+"output2"};
		MatrixLinkTwoEdge.main(path1);
		
		String [] path2 = {base+"output2",base+"output3"};
		MatrixLinkThreeEdge.main(path2);
	}
}
