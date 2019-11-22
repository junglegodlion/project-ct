package com.jungle.bigdata.ct.producer.bean;


import com.jungle.bigdata.ct.common.bean.Data;

/**
 * 联系人
 */
public class Contact extends Data {
    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * contact继承了Data
     * 所以存在setValue方法
     * 重写setValue方法
     * @param val
     */
    public void setValue(Object val) {
        content = (String)val;
        String[] values = content.split("\t");
        setName(values[1]);
        setTel(values[0]);
    }

    public String toString() {
        return "Contact["+tel+"，"+name+"]";
    }

}
