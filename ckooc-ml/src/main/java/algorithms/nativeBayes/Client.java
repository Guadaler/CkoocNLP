package algorithms.nativeBayes;

/**
 * 朴素贝叶斯算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        //训练集数据
        String filePath = "C:\\Users\\lyq\\Desktop\\icon\\input.txt";
        String testData = "Youth Medium Yes Fair";
        NaiveBayes tool = new NaiveBayes(filePath);
        System.out.println(testData + " 数据的分类为:" + tool.naiveBayesClassificate(testData));
    }
}
