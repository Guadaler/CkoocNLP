package algorithms.fpTree;

/**
 * FPTree频繁模式树算法
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        String filePath = "C:\\Users\\lyq\\Desktop\\icon\\testInput.txt";
        //最小支持度阈值
        int minSupportCount = 2;

        FPTree tool = new FPTree(filePath, minSupportCount);
        tool.startBuildingTree();
    }
}
