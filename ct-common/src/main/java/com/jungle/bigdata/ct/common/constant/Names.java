package com.jungle.bigdata.ct.common.constant;


import com.jungle.bigdata.ct.common.bean.Val;

/**
 * 名称常量枚举类
 */
public enum Names  {
    NAMESPACE("ct")
    ,TABLE("ct:calllog")
    ,CF_CALLER("caller") //主叫
    ,CF_CALLEE("callee") //被叫
    ,CF_INFO("info")
    ,TOPIC("calllog");


    private String name;

    /**
     * 构造器
     * @param name
     */
    private Names( String name ) {
        this.name = name;
    }
//
//
//    public void setValue(Object val) {
//       this.name = (String)val;
//    }

    /**
     * 取值
     * @return
     */
    public String getValue() {
        return name;
    }
}
