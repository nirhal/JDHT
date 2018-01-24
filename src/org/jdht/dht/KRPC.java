/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import org.jdht.bencode.Bencode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KRPC implements Runnable {

    public static final long DEFAULT_QUERY_TIMEOUT = 60*1000; // 60 sec

    public static interface OnResponseReceived {
        public void onResponseReceived(QueryData queryData, boolean error, Map response, List errorResponse) throws Exception;
        public void onTimeout(QueryData queryData);
    }

    public static interface OnQueryReceived {
        public Map onQueryReceived(ByteBuffer mID, InetAddress ip, int port, String method, Map args) throws Exception;
    }

    public static class QueryData {
        public ByteBuffer mID;
        public Node sentTo;
        public String method;
        public Map args;
        public OnResponseReceived onResponse;
        public long sentTime;
        public Object opaque;
    }

    private OnQueryReceived onQueryReceived = null;
    private int port;
    private DatagramSocket socket;
    private Thread thread = null;
    private final List<QueryData> queryList = new LinkedList<>();

    public KRPC(int port, OnQueryReceived onQueryReceived){
        this.port = port;
        this.onQueryReceived = onQueryReceived;
    }

    public void start() throws SocketException {
        socket = new DatagramSocket(this.port);
        thread = new Thread(this);
        thread.setName("DHT_KRPCReceiver");
        thread.start();
    }

    public void stop(){
        thread.interrupt();
        socket.close();
        thread = null;
    }

    private QueryData removeQuery(ByteBuffer mID, InetAddress ip, int port){
        synchronized (queryList){
            for (int i=0; i<queryList.size(); i++){
                QueryData queryData = queryList.get(i);
                if (queryData.mID.equals(mID) && queryData.sentTo.getIp().equals(ip) &&
                        queryData.sentTo.getPort() == port){
                    queryList.remove(i);
                    return queryData;
                }
            }
        }
        return null;
    }

    public void tick(){
        long now = System.currentTimeMillis();
        List<QueryData> timeoutQueries = new LinkedList<>();

        synchronized (queryList) {
            for (QueryData item : queryList){
                if (now > item.sentTime + DEFAULT_QUERY_TIMEOUT)
                    timeoutQueries.add(item);
            }

            queryList.removeAll(timeoutQueries);
        }

        for (QueryData item : timeoutQueries){
            if (item.onResponse != null)
                item.onResponse.onTimeout(item);
        }

    }

    public void sendQuery(Node node, String method, Map args, OnResponseReceived onResponse, Object opaque) throws Exception {
        QueryData queryData = new QueryData();

        queryData.mID = node.getMID();
        queryData.sentTo = node;
        queryData.method = method;
        queryData.args = args;
        queryData.onResponse = onResponse;
        queryData.sentTime = System.currentTimeMillis();
        queryData.opaque = opaque;

        synchronized (queryList) {
            queryList.add(queryData);
        }

        Map<ByteBuffer, Object> map = new HashMap<>();
        map.put(ByteBuffer.wrap("t".getBytes()), queryData.mID);
        map.put(ByteBuffer.wrap("y".getBytes()), ByteBuffer.wrap("q".getBytes()));
        map.put(ByteBuffer.wrap("q".getBytes()), ByteBuffer.wrap(method.getBytes()));
        map.put(ByteBuffer.wrap("a".getBytes()), args);

        Bencode bencode = new Bencode();
        bencode.setRootElement(map);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Bencode.getBencodeSize(map));
        bencode.print(baos);
        DatagramPacket p = new DatagramPacket(baos.toByteArray(), baos.size(), node.getIp(), node.getPort());
        socket.send(p);
    }

    private void receiveQuery(ByteBuffer mID, InetAddress ip, int port, String method, Map args) throws Exception{

        Map<ByteBuffer, Object> map = new HashMap<>();
        Map response = null;

        map.put(ByteBuffer.wrap("t".getBytes()), mID);

        if (onQueryReceived != null)
            response = onQueryReceived.onQueryReceived(mID, ip, port, method, args);

        if (response != null){
            map.put(ByteBuffer.wrap("y".getBytes()), ByteBuffer.wrap("r".getBytes()));
            map.put(ByteBuffer.wrap("r".getBytes()), response);
        } else {
            List<Object> error = new LinkedList<>();
            error.add(201L);
            error.add(ByteBuffer.wrap("A Generic Error Ocurred".getBytes()));
            map.put(ByteBuffer.wrap("y".getBytes()), ByteBuffer.wrap("e".getBytes()));
            map.put(ByteBuffer.wrap("e".getBytes()), error);
        }

        Bencode bencode = new Bencode();
        bencode.setRootElement(map);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Bencode.getBencodeSize(map));
        bencode.print(baos);
        DatagramPacket p = new DatagramPacket(baos.toByteArray(), baos.size(), ip, port);
        socket.send(p);
    }


    @Override
    public void run() {
        byte[] buf = new byte[64*1024];
        while(!Thread.currentThread().isInterrupted()) {

            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                socket.receive(p);
                ByteArrayInputStream bais = new ByteArrayInputStream(buf);
                Bencode bencode = new Bencode(bais);
                bais.close();
                Map map = (Map) bencode.getRootElement();
                ByteBuffer mID = (ByteBuffer) map.get(ByteBuffer.wrap("t".getBytes()));
                String type = new String(((ByteBuffer) map.get(ByteBuffer.wrap("y".getBytes()))).array());

                if (type.equals("q")){

                    String method = new String(((ByteBuffer) map.get(ByteBuffer.wrap("q".getBytes()))).array());
                    Map args = (Map) map.get(ByteBuffer.wrap("a".getBytes()));
                    receiveQuery(mID, p.getAddress(), p.getPort(), method, args);

                } else if (type.equals("r") || type.equals("e")) {

                    QueryData queryData = removeQuery(mID, p.getAddress(), p.getPort());

                    if (queryData != null){
                        if (type.equals("r")) {
                            Map response = (Map) map.get(ByteBuffer.wrap("r".getBytes()));
                            if (queryData.onResponse != null)
                                queryData.onResponse.onResponseReceived(queryData, false, response, null);
                        } else {
                            List response = (List) map.get(ByteBuffer.wrap("e".getBytes()));
                            if (queryData.onResponse != null)
                                queryData.onResponse.onResponseReceived(queryData, true, null, response);
                        }
                    }

                }



            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}

