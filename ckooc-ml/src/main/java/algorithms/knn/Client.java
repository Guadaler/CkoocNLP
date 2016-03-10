package algorithms.knn;

/**
 * K最近邻法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        String trainDataPath = "C:\\Users\\lyq\\Desktop\\icon\\trainInput.txt";
        String testDataPath = "C:\\Users\\lyq\\Desktop\\icon\\testinput.txt";

        KNN tool = new KNN(trainDataPath, testDataPath);
        tool.knnCompute(3);

    }
}
