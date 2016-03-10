package algorithms.id3;

/**
 * ID3决策树分类算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";

        ID3 tool = new ID3(filePath);
        tool.startBuildingTree(true);
    }
}
