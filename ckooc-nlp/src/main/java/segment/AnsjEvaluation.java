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
        String input = "G:/data/baike/test.txt";
        String output = "G:/data/baike/test_splited.txt";

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));

        String line;
        while ((line = br.readLine()) != null) {
            bw.write(NlpAnalysis.parse(line).toString() + "\n");
        }

        br.close();
        bw.close();
    }
}
