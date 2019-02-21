package com.andy.hadoop.mr.call;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * <p>1550425978144,18012350867,2018-05-29 01:52:58,802
 *
 * @author leone
 * @since 2019-02-21
 **/
public class CallBean implements Writable, Comparator<CallBean> {

    private String from;

    private String to;

    private String startTime;

    private int duration;

    private int sum;

    public CallBean(String from, String to, String startTime, int duration) {
        this.from = from;
        this.to = to;
        this.startTime = startTime;
        this.duration = duration;
    }

    public CallBean(String from, int sum) {
        this.from = from;
        this.sum = sum;
    }

    public CallBean() {
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(from);
        dataOutput.writeUTF(to);
        dataOutput.writeUTF(startTime);
        dataOutput.writeInt(duration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.from = dataInput.readUTF();
        this.to = dataInput.readUTF();
        this.startTime = dataInput.readUTF();
        this.duration = dataInput.readInt();
    }

    @Override
    public int compare(CallBean o1, CallBean o2) {
        return o2.getDuration() < o1.getDuration() ? 1 : 0;
    }

    @Override
    public String toString() {
        return "CallBean{" +
                "from='" + from + '\'' +
                ", sum=" + sum +
                '}';
    }
}
