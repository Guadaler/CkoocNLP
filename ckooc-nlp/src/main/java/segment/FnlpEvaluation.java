package segment;

import org.fnlp.nlp.cn.tag.CWSTagger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yhao on 2015/12/15.
 */
public class FnlpEvaluation implements Segmenter {
    private static CWSTagger tagger = null;
    static{
        try{
            tagger = new CWSTagger("ckooc-nlp/models/seg.m");
            tagger.setEnFilter(true);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, String> segMore(String text) {
        Map<String, String> map = new HashMap<>();
        map.put("Fnlp", tagger.tag(text));
        return map;
    }
}
