package algorithms.cart;

/**
 * CART分类回归树算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";

        CART tool = new CART(filePath);

        tool.startBuildingTree();
    }
}
