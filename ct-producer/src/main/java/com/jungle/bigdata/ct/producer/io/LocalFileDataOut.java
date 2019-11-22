package com.jungle.bigdata.ct.producer.io;


import com.jungle.bigdata.ct.common.bean.DataOut;

import java.io.*;

/**
 * 本地文件数据输出
 */
public class LocalFileDataOut implements DataOut {

    //输出字符流
    private PrintWriter writer = null;

    /**
     * 构造器
     * @param path
     */
    public LocalFileDataOut( String path ) {
        setPath(path);
    }

    /**
     * 设置路径
     * @param path
     */
    public void setPath(String path) {
        try {
            //true表示追加，不加的话，没次启动都将覆盖重写
//            writer = new PrintWriter( new OutputStreamWriter( new FileOutputStream(path,true), "UTF-8"));
            //FileOutputStream 输出字节流
            writer = new PrintWriter( new OutputStreamWriter( new FileOutputStream(path), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(Object data) throws Exception {
        write2(data.toString());
    }

    /**
     * 将数据字符串生成到文件中
     * @param data
     * @throws Exception
     */
    public void write2(String data) throws Exception {
        //写入数据
        writer.println(data);
//        输出流每次写一条日之后需要flush，不然可能导致积攒多条数据才输出一次。
        writer.flush();
    }

    /**
     * 释放资源
     * @throws IOException
     */
    public void close() throws IOException {

        if ( writer != null ) {
            writer.close();
        }
    }
}
