package algorithms.pageRank;

/**
 * PageRank计算网页重要性/排名算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		
		PageRank tool = new PageRank(filePath);
		tool.printPageRankValue();
	}
}
