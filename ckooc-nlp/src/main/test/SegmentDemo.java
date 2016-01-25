import segment.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yhao on 2015/12/15.
 */
public class SegmentDemo {
    public static Map<String, Set<String>> contrast(String text){
        Map<String, Set<String>> map = new LinkedHashMap<>();
//        map.put("Stanford分词器", new StandfordEvaluation().seg(text));
        map.put("HanLP分词器", new HanlpEvaluation().seg(text));
        map.put("FudanNLP分词器", new FnlpEvaluation().seg(text));
        map.put("Jieba分词器", new JiebaEvaluation().seg(text));
//        map.put("Jcseg分词器", new JcsegEvaluation().seg(text));
//        map.put("MMSeg4j分词器", new MMSeg4jEvaluation().seg(text));
//        map.put("IKAnalyzer分词器", new IKAnalyzerEvaluation().seg(text));
//        map.put("smartcn分词器", new SmartCNEvaluation().seg(text));
        return map;
    }
    public static Map<String, Map<String, String>> contrastMore(String text){
        Map<String, Map<String, String>> map = new LinkedHashMap<>();
//        map.put("Stanford分词器", new StandfordEvaluation().segMore(text));
        map.put("HanLP分词器", new HanlpEvaluation().segMore(text));
        map.put("FudanNLP分词器", new FnlpEvaluation().segMore(text));
        map.put("Jieba分词器", new JiebaEvaluation().segMore(text));
//        map.put("Jcseg分词器", new JcsegEvaluation().segMore(text));
//        map.put("MMSeg4j分词器", new MMSeg4jEvaluation().segMore(text));
//        map.put("IKAnalyzer分词器", new IKAnalyzerEvaluation().segMore(text));
//        map.put("smartcn分词器", new SmartCNEvaluation().segMore(text));
        return map;
    }

    public static void show(Map<String, Set<String>> map){
        map.keySet().forEach(k -> {
            System.out.println("\n" + k + " 的分词结果：");
            AtomicInteger i = new AtomicInteger();
            map.get(k).forEach(v -> {
                System.out.println("\t" + i.incrementAndGet() + " 、" + v);
            });
        });
    }
    public static void showMore(Map<String, Map<String, String>> map){
        map.keySet().forEach(k->{
            System.out.println("\n" + k + " 的分词结果：");
            AtomicInteger i = new AtomicInteger();
            map.get(k).keySet().forEach(a -> {
                System.out.println("\t" + i.incrementAndGet()+ " 、【"   + a + "】\t" + map.get(k).get(a));
            });
        });
    }
    public static void main(String[] args) {
        show(contrast("江州市长江大桥参加了长江大桥的通车仪式"));
        System.out.println("\n*************************************");
        showMore(contrastMore("江州市长江大桥参加了长江大桥的通车仪式"));
    }
}
