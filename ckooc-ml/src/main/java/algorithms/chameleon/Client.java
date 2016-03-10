package algorithms.chameleon;

/**
 * Chameleon(变色龙)两阶段聚类算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\graphData.txt";
		//k-近邻的k设置
		int k = 1;
		//度量函数阈值
		double minMetric = 0.1;
		
		ChameleonTool tool = new ChameleonTool(filePath, k, minMetric);
		tool.buildCluster();
	}
}
