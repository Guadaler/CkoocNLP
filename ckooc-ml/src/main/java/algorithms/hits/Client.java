package algorithms.hits;

/**
 * HITS链接分析算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		
		HITS tool = new HITS(filePath);
		tool.printResultPage();
	}
}
