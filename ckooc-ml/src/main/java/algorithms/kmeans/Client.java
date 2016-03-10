package algorithms.kmeans;

/**
 * KMeans算法Demo
 * Created by yhao on 2016/3/10.
 */
public class Client {
    public static void main(String[] args){
        String filePath = "./input.txt";
        //聚类中心数量设定
        int classNum = 3;

        KMeans tool = new KMeans(filePath, classNum);
        tool.kMeansClustering();
    }
}
