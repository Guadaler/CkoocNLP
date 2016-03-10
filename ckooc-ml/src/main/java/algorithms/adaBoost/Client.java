package algorithms.adaBoost;

/**
 * AdaBoost提升算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] agrs){
        String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
        //误差率阈值
        double errorValue = 0.2;

        AdaBoost tool = new AdaBoost(filePath, errorValue);
        tool.adaBoostClassify();
    }
}
