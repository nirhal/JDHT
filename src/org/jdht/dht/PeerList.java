/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PeerList {

    public static final long CLEAN_INTERVAL = 60*1000; // 60 sec
    public static final long PEER_EXPIRE_TIME = 15*60*1000; // 15 min
    private final static int MAX_INFOHASHES = 20;
    private final static int MAX_PEERS_PER_INFOHASH = 10;

    private Map<ByteBuffer, List<PeerInfo>> peerMap = new HashMap<>();
    private long lastCleanTime = 0;

    public PeerList(){
        lastCleanTime = System.currentTimeMillis();
    }

    public synchronized void announce(ByteBuffer infoHash, PeerInfo newPeer){
        if (peerMap.size() >= MAX_INFOHASHES)
            return;
        List<PeerInfo> peers = peerMap.get(infoHash);
        if (peers == null){
            peers = new LinkedList<>();
            peerMap.put(infoHash, peers);
        }

        if (peers.size() >= MAX_PEERS_PER_INFOHASH)
            return;

        for (PeerInfo peer : peers){
            if (newPeer.getIp().equals(peer.getIp()) && newPeer.getPort() == peer.getPort()){
                peer.setLastSeen();
                return;
            }
        }

        peers.add(newPeer);
    }

    public synchronized List<PeerInfo> getPeers(ByteBuffer infoHash){
        List<PeerInfo> peers = peerMap.get(infoHash);
        if (peers == null || peers.size() == 0)
            return null;

        return Collections.unmodifiableList(peers);
    }

    public synchronized void clear(){
        for (List<PeerInfo> peerList : peerMap.values())
            peerList.clear();
        peerMap.clear();
    }

    public synchronized void tick(){
        long now = System.currentTimeMillis();
        if (lastCleanTime < now - CLEAN_INTERVAL) {
            lastCleanTime = now;

            int totalPeers = 0;
            int totalRemovedPeers = 0;
            List<ByteBuffer> removeHashes = new LinkedList<>();
            List<PeerInfo> removePeers = new LinkedList<>();
            for (Map.Entry<ByteBuffer, List<PeerInfo>> entry : peerMap.entrySet()){
                totalPeers += entry.getValue().size();
                removePeers.clear();
                for (PeerInfo peer : entry.getValue())
                    if (peer.getLastSeen() < now - PEER_EXPIRE_TIME)
                        removePeers.add(peer);

                entry.getValue().removeAll(removePeers);
                totalRemovedPeers += removePeers.size();
                if (entry.getValue().size() == 0)
                    removeHashes.add(entry.getKey());

            }

            for (ByteBuffer hash : removeHashes)
                peerMap.remove(hash);

            if (Logger.verbose)
                Logger.log("Clean peer list: " + totalRemovedPeers + " out of " + totalPeers + " removed. " +
                        "Have " + peerMap.size() + " infohashes.");

        }
    }

}
