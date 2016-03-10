package algorithms.em;

/**
 * EM期望最大化算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
	public static void main(String[] args){
		String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
		
		EM tool = new EM(filePath);
		tool.readDataFile();
		tool.exceptMaxStep();
	}
}
