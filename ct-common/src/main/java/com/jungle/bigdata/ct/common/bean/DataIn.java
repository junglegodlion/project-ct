package com.jungle.bigdata.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 数据来源
 */
public interface DataIn extends Closeable {
    public void setPath(String path);

    /**
     * 读取数据
     * 接口中就事先抛出异常
     * @return
     * @throws IOException
     */
    public Object read() throws IOException;

    /**
     * 使用泛型
     * 实现传进来什么类型，就返回什么类型
     * T extends Data 能封装成对象的数据，T是Data的子类
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}
