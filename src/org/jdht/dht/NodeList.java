/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class NodeList {

    public static final long CLEAN_INTERVAL = 60*1000; // 60 sec
    public static final long NODE_EXPIRE_TIME = 15*60*1000; // 15 min
    public static final long NODE_REPLACEABLE_TIME = 12*60*1000; // 12 min
    public static final long NODE_PING_TIME = 10*60*1000; // 10 min
    public static final long EXPLORE_INTERVAL = 60*1000; // 60 sec
    public static final long UPDATE_BUCKET_INTERVAL = 10*60*1000; // 10 min
    public static final int EXPLORE_AGGRESSIVE_MAX_NODES = 100;
    public static final int EXPLORE_MAX_NODES = 600;
    public static final int MAX_NODES = 800;

    private final HashMap<ByteBuffer, Node> nodeMap = new HashMap<>();
    private final BucketSet bucketSet;
    private long lastCleanTime = 0;
    private long lastExploreTime = 0;
    private ByteBuffer myNodeID;

    public NodeList(ByteBuffer myNodeID){
        lastCleanTime = System.currentTimeMillis();
        bucketSet = new BucketSet(myNodeID, new BucketTrimmer(this));
        this.myNodeID = myNodeID;
    }

    public synchronized void clear(){
        nodeMap.clear();
        bucketSet.clear();
    }

    public synchronized int size(){
        return nodeMap.size();
    }

    public synchronized int numOfBuckets(){
        return bucketSet.getBuckets().size();
    }

    public synchronized Node get(ByteBuffer nid) {
        return nodeMap.get(nid);
    }

    public synchronized Node putIfAbsent(ByteBuffer nodeId, InetAddress ip, int port, boolean isPermanent, boolean returnAnyway) {
        if (nodeMap.size() >= MAX_NODES || nodeId.equals(myNodeID))
            return returnAnyway ? new Node(nodeId, ip, port, isPermanent) : null;
        if (nodeMap.containsKey(nodeId)) {
            Node node = nodeMap.get(nodeId);
            if (!ip.equals(node.getIp()) || port != node.getPort())
                return returnAnyway ? node : null;
            node.setPermanent(isPermanent);
            return node;
        } else {
            Node node = new Node(nodeId, ip, port, isPermanent);
            if (bucketSet.add(nodeId)) {
                nodeMap.put(nodeId, node);
                return node;
            }
            return returnAnyway ? node : null;
        }
    }

    public synchronized void put(Node node){
        if (bucketSet.add(node.getNodeId()))
            nodeMap.put(node.getNodeId(), node);
    }

    public synchronized Node remove(ByteBuffer nodeId){
        Node node = nodeMap.get(nodeId);
        if (node != null && !node.isPermanent()){
            bucketSet.remove(nodeId);
            return nodeMap.remove(nodeId);
        }
        return null;
    }

    public synchronized List<Node> findClosest(ByteBuffer id, int max) {
        List<ByteBuffer> ids = bucketSet.getClosest(id, 2*max);
        List<Node> closest = new ArrayList<Node>(ids.size());
        // Add non-questionable first
        for (ByteBuffer key : ids) {
            Node node = nodeMap.get(key);
            if (node != null && !node.isQuestionable() && closest.size()<max) {
                closest.add(node);
            }
        }
        // Add questionable if there is room
        if (closest.size()>=max)
            return closest;
        for (ByteBuffer key : ids) {
            Node node = nodeMap.get(key);
            if (node != null && node.isQuestionable() && closest.size()<max) {
                closest.add(node);
            }
        }
        return closest;
    }

    public synchronized void tick(DHT dht){
        long now = System.currentTimeMillis();

        // clean list
        if (lastCleanTime < now - CLEAN_INTERVAL) {
            lastCleanTime = now;

            List<ByteBuffer> removeNID = new LinkedList<>();

            for (Node node : nodeMap.values()) {
                if (node.getLastSeen() < now - NODE_EXPIRE_TIME) {
                    removeNID.add(node.getNodeId());
                } else if (node.getLastSeen() < now - NODE_PING_TIME || node.isQuestionable()) {
                    dht.sendPing(node);
                    if (Logger.verbose)
                        Logger.log("Verifying node: Ping to " + node.getIp().toString());
                }
            }

            for (ByteBuffer key : removeNID) {
                remove(key);
                dht.addToBlackList(key);
            }

            if (Logger.verbose)
                Logger.log("Clean node list: " + removeNID.size() + " removed.");
        }

        // explore
        if (lastExploreTime < now - EXPLORE_INTERVAL && nodeMap.size()>0){
            lastExploreTime = now;

            if (nodeMap.size() <= EXPLORE_MAX_NODES) {
                for (Bucket b : bucketSet.getBuckets()){
                    // update old and not full buckets
                    if (b.getLastChanged() < now - UPDATE_BUCKET_INTERVAL && (b.size() < 6 || b.getRangeBegin() != b.getRangeEnd())) {
                        if (Logger.verbose)
                            Logger.log("Explore in bucket " + b.getRangeBegin() + " - " + b.getRangeEnd());
                        // check closest bucket
                        if (b.getRangeBegin() != b.getRangeEnd()) {
                            // explore my ID
                            int depth = 2;
                            if (nodeMap.size() < EXPLORE_AGGRESSIVE_MAX_NODES)
                                depth = 3;
                            dht.explore(myNodeID, 8, depth);
                            b.setLastChanged();
                        } else {
                            // generate random ID
                            ByteBuffer randomID = IDGenerator.generateRandomID(myNodeID, b.getRangeBegin());
                            dht.explore(randomID, 8, 1);
                            b.setLastChanged();
                        }

                    }
                }

            }
        }
    }

    private static class BucketTrimmer implements BucketSet.Trimmer {

        private WeakReference<NodeList> listWeakReference;

        public BucketTrimmer(NodeList list){
            listWeakReference = new WeakReference<NodeList>(list);
        }

        @Override
        public boolean trim(Bucket b) {
            NodeList list = listWeakReference.get();
            if (list == null)
                return false;

            long now = System.currentTimeMillis();

            List<ByteBuffer> removeNID = new LinkedList<>();
            for (ByteBuffer id : b.getEntries()){
                Node node = list.get(id);
                if (node == null || node.getLastSeen() < now - NODE_REPLACEABLE_TIME)
                    removeNID.add(id);
            }

            for (ByteBuffer key : removeNID) {
                list.remove(key);
            }

            return !b.isFull();
        }
    }

}
