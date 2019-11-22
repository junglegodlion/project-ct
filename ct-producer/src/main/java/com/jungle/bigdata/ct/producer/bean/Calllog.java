package com.jungle.bigdata.ct.producer.bean;

/**
 * 通话日志对象
 */
public class Calllog {
    private String call1;
    private String call2;
    private String calltime;
    private String duration;

    /**
     * 构造器
     * @param call1
     * @param call2
     * @param calltime
     * @param duration
     */
    public Calllog(String call1, String call2, String calltime, String duration) {
        this.call1 = call1;
        this.call2 = call2;
        this.calltime = calltime;
        this.duration = duration;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    /**
     * 重写tostring方法
     * @return
     */
    @Override
    public String toString() {
        return call1 + "\t" + call2 + "\t" + calltime + "\t" + duration;
    }
}
