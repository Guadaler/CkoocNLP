package algorithms.apriori;

/**
 * Apriori算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        String filePath = "./testInput.txt";

        Apriori tool = new Apriori(filePath, 2);
        tool.printAttachRule(0.7);
    }
}
