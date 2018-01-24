/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import java.net.InetAddress;
import java.nio.ByteBuffer;

public class Node {

    private static final int MAX_MESSAGE_ID = 0xffff;

    private InetAddress ip = null;
    private ByteBuffer nodeId;
    private int port;
    private int mID = 0;
    private boolean permanent = false;
    private long lastSeen = 0;
    private byte[] compactInfo = null;
    private boolean questionable = false;

    public Node(ByteBuffer nodeId, InetAddress ip, int port, boolean permanent){
        this.ip = ip;
        this.nodeId = nodeId;
        this.port = port;
        this.permanent = permanent;
        if (nodeId != null && ip != null){
            ByteBuffer cibb = ByteBuffer.allocate(nodeId.array().length + ip.getAddress().length + 2);
            cibb.put(nodeId.array());
            cibb.put(ip.getAddress());
            cibb.put((byte) ((port >> 8) & 0xff));
            cibb.put((byte) (port & 0xff));
            compactInfo = cibb.array();
        }
    }

    public Node(ByteBuffer nodeId, InetAddress ip, int port){
        this(nodeId, ip, port, false);
    }

    public Node(InetAddress ip, int port, boolean permanent){
        this(null, ip, port, permanent);
    }

    public Node(InetAddress ip, int port){
        this(null, ip, port, false);
    }

    public Node(int port){
        this.nodeId = IDGenerator.generateRandomID();
        this.port = port;
    }

    public Node(ByteBuffer nodeId, int port){
        this.nodeId = nodeId;
        this.port = port;
    }

    public InetAddress getIp() {
        return ip;
    }

    public ByteBuffer getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public byte[] getCompactInfo() {
        return compactInfo;
    }

    public synchronized void setPermanent(boolean permanent){
        this.permanent = permanent;
    }

    public synchronized boolean isPermanent() {
        return permanent;
    }

    public synchronized void setLastSeen(){
        lastSeen = System.currentTimeMillis();
    }

    public synchronized void setLastSeen(long time){
        lastSeen = time;
    }

    public synchronized long getLastSeen(){
        return lastSeen;
    }

    public synchronized void setQuestionable(boolean questionable){
        this.questionable = questionable;
    }

    public synchronized boolean isQuestionable(){
        return questionable;
    }

    public synchronized ByteBuffer getMID(){
        mID = (mID + 1) % MAX_MESSAGE_ID;
        byte[] bytes = {(byte) (mID & 0xFF), (byte) ((mID & 0xFF00) >>> 8)};
        return ByteBuffer.wrap(bytes);
    }
}
