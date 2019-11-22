package com.jungle.bigdata.ct.common.bean;


/**
 * 值对象接口
 */
public interface Val {
    /**
     * 设置值
     * @param val
     */
    public void setValue( Object val );

    /**
     * 取值
     * @return
     */
    public Object getValue();
}

