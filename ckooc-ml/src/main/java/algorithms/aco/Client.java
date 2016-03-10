package algorithms.aco;

/**
 * 蚁群算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		//测试数据
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		//蚂蚁数量
		int antNum;
		//蚁群算法迭代次数
		int loopCount;
		//控制参数
		double alpha;
		double beita;
		double p;
		double Q;
		
		antNum = 3;
		alpha = 0.5;
		beita = 1;
		p = 0.5;
		Q = 5;
		loopCount = 5;
		
		ACO tool = new ACO(filePath, antNum, alpha, beita, p, Q);
		tool.antStartSearching(loopCount);
	}
}
