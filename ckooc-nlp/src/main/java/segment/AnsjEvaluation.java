package segment;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yhao on 2015/12/15.
 */
public class AnsjEvaluation implements Segmenter {
    @Override
    public Map<String, String> segMore(String text) {
        Map<String, String> map = new HashMap<>();

        StringBuilder result = new StringBuilder();
        for (Term term : BaseAnalysis.parse(text)) {
            result.append(term.getName()).append(" ");
        }
        map.put("BaseAnalysis", result.toString());

        result.setLength(0);
        for (Term term : ToAnalysis.parse(text)) {
            result.append(term.getName()).append(" ");
        }
        map.put("ToAnalysis", result.toString());

        result.setLength(0);
        for(Term term : NlpAnalysis.parse(text)){
            result.append(term.getName()).append(" ");
        }
        map.put("NlpAnalysis", result.toString());

        result.setLength(0);
        for(Term term : IndexAnalysis.parse(text)){
            result.append(term.getName()).append(" ");
        }
        map.put("IndexAnalysis", result.toString());

        return map;
    }

    public static void main(String[] args)throws IOException {
        String input = "G:/test/sample.txt";
        String output = "G:/result/test_splited.txt";

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));

        /*String line;
        while ((line = br.readLine()) != null) {
            bw.write(ToAnalysis.parse(line).toString() + "\n");
        }*/

        List<Term> parse = NlpAnalysis.parse("洁面仪配合洁面深层清洁毛孔 清洁鼻孔面膜碎觉使劲挤才能出一点点皱纹 脸颊毛孔修复的看不见啦 草莓鼻历史遗留问题没辙 脸和脖子差不多颜色的皮肤才是健康的 长期使用安全健康的比同龄人显小五到十岁 28岁的妹子看看你们的鱼尾纹");
        System.out.println(parse);

        br.close();
        bw.close();
    }
}
