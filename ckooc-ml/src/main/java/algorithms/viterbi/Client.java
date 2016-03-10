package algorithms.viterbi;

/**
 * 维特比算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args) {
		// 状态转移概率矩阵路径
		String stmFilePath;
		// 混淆矩阵路径
		String cfFilePath;
		// 观察到的状态
		String[] observeStates;
		// 初始状态
		double[] initStatePro;
		Viterbi tool;

		stmFilePath = "C:\\Users\\lyq\\Desktop\\icon\\stmatrix.txt";
		cfFilePath = "C:\\Users\\lyq\\Desktop\\icon\\humidity-matrix.txt";

		initStatePro = new double[] { 0.63, 0.17, 0.20 };
		observeStates = new String[] { "Dry", "Damp", "Soggy" };

		tool = new Viterbi(stmFilePath, cfFilePath, initStatePro,
				observeStates);
		tool.calHMMObserve();
	}
}
