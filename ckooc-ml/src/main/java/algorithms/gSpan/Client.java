package algorithms.gSpan;

/**
 * gSpan频繁子图挖掘算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		//测试数据文件地址
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		//最小支持度率
		double minSupportRate = 0.2;
		
		GSpan tool = new GSpan(filePath, minSupportRate);
		tool.freqGraphMining();
	}
}
