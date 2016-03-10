package algorithms.ga;

/**
 * Genetic遗传算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		//变量最小值和最大值
		int minNum = 1;
		int maxNum = 7;
		//初始群体规模
		int initSetsNum = 4;
		
		GA tool = new GA(minNum, maxNum, initSetsNum);
		tool.geneticCal();
	}
}
