package segment;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 获取文本的所有分词结果, 对比不同分词器结果
 * Created by yhao on 2015/12/15.
 */
public interface Segmenter {
    /**
     * 获取文本的所有分词结果
     * @param text  输入文本
     * @return  所有分词结果，去除重复词
     */
    default Set<String> seg(String text) {
        return segMore(text).values().stream().collect(Collectors.toSet());
    }

    /**
     * 获取文本的所有分词结果
     * @param text  输入文本
     * @return  所有分词结果，KEY为分词器模式，VALUE为分词器结果
     */
    public Map<String, String> segMore(String text);
}
