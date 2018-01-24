/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PeerInfo {

    private InetAddress ip = null;
    private int port;
    private long lastSeen;
    private byte[] compactInfo = null;
    private int hash = 0;

    public PeerInfo(InetAddress ip, int port){
        this.ip = ip;
        this.port = port;
        setLastSeen();
        ByteBuffer cibb = ByteBuffer.allocate(ip.getAddress().length + 2);
        cibb.put(ip.getAddress());
        cibb.put((byte) ((port >> 8) & 0xff));
        cibb.put((byte) (port & 0xff));
        compactInfo = cibb.array();
        computeHash();
    }

    public PeerInfo(ByteBuffer buffer, int ipLength) throws UnknownHostException {
        byte[] peerIPBA = new byte[ipLength];
        buffer.get(peerIPBA);
        byte[] peerPortBA = new byte[2];
        buffer.get(peerPortBA);
        ip = InetAddress.getByAddress(peerIPBA);
        port = ((peerPortBA[0] & 0xFF) << 8) | (peerPortBA[1] & 0xFF);
        setLastSeen();
        compactInfo = new byte[ipLength + 2];
        System.arraycopy(peerIPBA, 0, compactInfo, 0, ipLength);
        System.arraycopy(peerPortBA, 0, compactInfo, ipLength, 2);
        computeHash();
    }

    private void computeHash(){
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            byte[] bytes = md.digest(compactInfo);
            hash = bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
        } catch (NoSuchAlgorithmException ex) {
            for (int i=0; i<compactInfo.length; i++)
                hash = hash + compactInfo[i];
        }
    }

    public InetAddress getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public byte[] getCompactInfo() {
        return compactInfo;
    }

    public synchronized void setLastSeen(){
        lastSeen = System.currentTimeMillis();
    }

    public synchronized long getLastSeen(){
        return lastSeen;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PeerInfo))
            return false;

        PeerInfo other = (PeerInfo) obj;

        return ip.equals(other.getIp()) && port == other.getPort();
    }
}
