package segment;

import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.SpeedTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhao on 2015/12/15.
 */
public class HanlpEvaluation implements Segmenter {
    private static final Segment N_SHORT_SEGMENT = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
    private static final Segment DIJKSTRA_SEGMENT = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);

    @Override
    public Map<String, String> segMore(String text) {
        Map<String, String> map = new HashMap<>();
        map.put("标准分词", standard(text));
        map.put("NLP分词", nlp(text));
        map.put("索引分词", index(text));
        map.put("N-最短路径分词", nShort(text));
        map.put("最短路径分词", shortest(text));
        map.put("极速词典分词", speed(text));
        return map;
    }

    private static String standard(String text) {
        StringBuilder result = new StringBuilder();
        StandardTokenizer.segment(text).forEach(term->result.append(term.word).append(" "));
        return result.toString();
    }
    private static String nlp(String text) {
        StringBuilder result = new StringBuilder();
        NLPTokenizer.segment(text).forEach(term->result.append(term.word).append(" "));
        return result.toString();
    }
    private static String index(String text) {
        StringBuilder result = new StringBuilder();
        IndexTokenizer.segment(text).forEach(term->result.append(term.word).append(" "));
        return result.toString();
    }
    private static String speed(String text) {
        StringBuilder result = new StringBuilder();
        SpeedTokenizer.segment(text).forEach(term->result.append(term.word).append(" "));
        return result.toString();
    }
    private static String nShort(String text) {
        StringBuilder result = new StringBuilder();
        N_SHORT_SEGMENT.seg(text).forEach(term->result.append(term.word).append(" "));
        return result.toString();
    }
    private static String shortest(String text) {
        StringBuilder result = new StringBuilder();
        DIJKSTRA_SEGMENT.seg(text).forEach(term->result.append(term.word).append(" "));
        return result.toString();
    }

    public static void main(String[] args) {
        HanlpEvaluation hanlpEvaluation = new HanlpEvaluation();
        String text = "江州市长江大桥参加了长江大桥的通车仪式";
        Map<String, String> segments = hanlpEvaluation.segMore(text);

        for (String segment : segments.keySet()) {
            System.out.println(segment + ": " + segments.get(segment));
        }
    }
}
