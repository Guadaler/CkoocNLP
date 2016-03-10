package algorithms.dbScan;

/**
 * Dbscan基于密度的聚类算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		//簇扫描半径
		double eps = 3;
		//最小包含点数阈值
		int minPts = 3;
		
		DBScan tool = new DBScan(filePath, eps, minPts);
		tool.dbScanCluster();
	}
}
