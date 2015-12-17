package segment;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.SegToken;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhao on 2015/12/15.
 */
public class JiebaEvaluation implements Segmenter {
    private static final JiebaSegmenter JIEBA_SEGMENTER = new JiebaSegmenter();

    @Override
    public Map<String, String> segMore(String text) {
        Map<String, String> map = new HashMap<>();
        map.put("INDEX", seg(text, SegMode.INDEX));
        map.put("SEARCH", seg(text, SegMode.SEARCH));
        return map;
    }

    private static String seg(String text, SegMode segMode) {
        StringBuilder result = new StringBuilder();
        for(SegToken token : JIEBA_SEGMENTER.process(text, segMode)){
            result.append(token.word).append(" ");
        }
        return result.toString();
    }
}
