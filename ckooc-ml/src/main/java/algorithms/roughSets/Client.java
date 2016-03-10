package algorithms.roughSets;

/**
 * 粗糙集约简算法
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		
		RoughSets tool = new RoughSets(filePath);
		tool.findingReduct();
	}
}
