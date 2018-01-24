/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PeerQuery {

    private static final int MAX_ANNOUNCE_NODES = 8;
    private static final int MAX_QUERY_TIME = 60*1000; // 60 sec

    private Set<ByteBuffer> tryingNodes;
    private Map<ByteBuffer, Node> nodesTried;
    private Map<ByteBuffer, Object> tokens;
    private Set<PeerInfo> peers;
    private long startedTime;
    private int maxPeers;
    private boolean announce;
    private ByteBuffer infoHash;
    private int originalDepth;

    public PeerQuery(ByteBuffer infoHash, int maxPeers, boolean announce, int originalDepth){
        this.infoHash = infoHash;
        this.maxPeers = maxPeers;
        this.announce = announce;
        this.originalDepth = originalDepth;
        startedTime = System.currentTimeMillis();
        nodesTried = new HashMap<>();
        tokens = new HashMap<>();
        peers = new HashSet<>();
        tryingNodes = new HashSet<>();
    }

    public synchronized void addTriedNode(Node node, Object token){
        tryingNodes.remove(node.getNodeId());
        nodesTried.put(node.getNodeId(), node);
        tokens.put(node.getNodeId(), token);
    }

    public synchronized boolean isAlreadyTried(ByteBuffer nodeId){
        boolean ret = nodesTried.get(nodeId) != null || tryingNodes.contains(nodeId);
        if (!ret)
            tryingNodes.add(nodeId);
        return ret;
    }

    public synchronized int getTriedNodesCount(){
        return nodesTried.size() + tryingNodes.size();
    }

    public synchronized List<PeerInfo> addPeers(List<PeerInfo> newPeers){
        List<PeerInfo> result = new LinkedList<>();
        for (PeerInfo peer : newPeers){
            if (!peers.contains(peer))
                result.add(peer);
        }
        peers.addAll(result);
        return result;
    }

    public synchronized Map<Node, Object> getAnnounceNodes(){
        if (!announce)
            return null;

        announce = false; // Don't announce twice

        // Find closest nodes
        List<ByteBuffer> list = new LinkedList<>();
        list.addAll(nodesTried.keySet());
        Collections.sort(list, new BucketSet.XORComparator(infoHash));

        Map<Node, Object> nodeList = new LinkedHashMap<>(MAX_ANNOUNCE_NODES);
        for (ByteBuffer nid : list){
            if (nodeList.size() < MAX_ANNOUNCE_NODES){
                nodeList.put(nodesTried.get(nid), tokens.get(nid));
            } else {
                break;
            }
        }

        return nodeList;

    }

    public synchronized boolean shouldContinue(){
        long now = System.currentTimeMillis();
        return startedTime > now - MAX_QUERY_TIME && peers.size() < maxPeers;
    }

    public ByteBuffer getInfoHash() {
        return infoHash;
    }

    public boolean isAnnounce() {
        return announce;
    }

    public int getOriginalDepth() {
        return originalDepth;
    }
}
