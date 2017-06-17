package com.mobin.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Mobin on 2017/6/17.
 */

/**
 *
 * @param <K>:文件名
 * @param <V>：时间戳
 */
public class SmallLRUCache<K, V> extends LinkedHashMap<K , V>{
    private final int MaxSize;

    public SmallLRUCache(int MaxSize) {
        super(MaxSize, (float)0.75, true); //true:队列中的元素为插入顺序，false:队列中的元素为访问顺序
        this.MaxSize = MaxSize;
    }

    //在队列中的元素超过MaxSize时将移除最旧的元素（如果是访问顺序则移除最先访问的元素）
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > MaxSize;
    }
}
