package com.jungle.bigdata.ct.common.bean;

/**
 * 数据对象
 * 不能实例化，只能被继承
 */
public abstract class Data implements Val {

    //数据
    public String content;


    /**
     * 给数据赋值
     * @param val
     */
    public void setValue(Object val) {
        content = (String)val;
    }

    /**
     * 取值
     * @return
     */
    public String getValue() {
        return content;
    }
}
