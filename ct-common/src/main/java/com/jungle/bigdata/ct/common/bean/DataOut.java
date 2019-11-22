package com.jungle.bigdata.ct.common.bean;

import java.io.Closeable;

/**
 * 数据去哪
 */
public interface DataOut extends Closeable {
    public void setPath(String path);

    public void write(Object data) throws Exception;
    public void write2(String data) throws Exception;
}
