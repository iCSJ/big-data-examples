package com.andy.storm.util;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-28
 **/
public class CommonUtil {

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 进程信息
     *
     * @return
     */
    public static String getPID() {
        String info = ManagementFactory.getRuntimeMXBean().getName();
        return info.split("@")[0];
    }

    /**
     * 线程信息
     *
     * @return
     */
    public static String getTID() {
        return Thread.currentThread().getName();
    }


    /**
     * 对象信息
     *
     * @param object
     * @return
     */
    public static Object getOID(Object object) {
        return object.getClass().getSimpleName() + "@" + object.hashCode();
    }

    public static String info(Object object, String message) {
        return getHostname() + "," + getPID() + "," + getTID() + "," + getOID(object);

    }

    public static void main(String[] args) {
        String hello = CommonUtil.info(new String("james"), "hello");
        System.out.println(hello);

    }

    /**
     * 发送套接字消息
     *
     * @param object
     * @param message
     */
    public static void sendToClient(Object object, String message, Integer port) {
        try {
            Socket socket = new Socket("node-2", port);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write((info(object, message) + "\r\n").getBytes());
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
