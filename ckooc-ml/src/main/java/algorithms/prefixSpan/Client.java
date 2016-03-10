package algorithms.prefixSpan;

/**
 * PrefixSpan序列模式挖掘算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] agrs){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		//最小支持度阈值率
		double minSupportRate = 0.4;
		
		PrefixSpan tool = new PrefixSpan(filePath, minSupportRate);
		tool.prefixSpanCalculate();
	}
}

