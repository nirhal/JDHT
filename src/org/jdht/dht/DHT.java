/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DHT implements Runnable {

    private final static long TICK_THREAD_SLEEP = 5000; // 5 sec
    private final static int NUM_RETURN_NODES = 8;
    private final static long EXPLORER_NODE_INTERVAL = 3000; // 3 sec
    private final static long BLACKLIST_CLEAN_INTERVAL = 60*1000; // 60 sec
    private final static long TOKENS_CLEAN_INTERVAL = 65*1000; // 65 sec
    private final static long ANNOUNCE_INTERVAL = 70*1000; // 70 sec
    private final static long BLACKLIST_TIMEOUT = 10*60*1000; // 10 mins
    private final static long TOKEN_TIMEOUT = 10*60*1000; // 10 mins
    private final static int DEFAULT_GETPEERS_DEPTH = 5;
    private final static int DEFAULT_GETPEERS_MAXPEERS = 50;

    private static class GetPeersOpaque {
        public int depth;
        public PeerQuery peerQuery;
    }

    private Node myNode = null;
    private KRPC krpc = null;
    private ResponseReceiver responseReceiver;
    private NodeList nodeList;
    private PeerList peerList;
    private final HashMap<ByteBuffer, Long> blacklist;
    private final HashMap<ByteBuffer, Token> outgoingTokens;
    private final List<PeerQuery> peerQueries;
    private long lastBlacklistClean = 0;
    private long lastTokensClean = 0;
    private long lastAnnounce = 0;
    private Thread thread;
    private int peerPort;
    private AtomicBoolean stopped = new AtomicBoolean(true);
    private DHTPeersReceiver peersReceiver;

    public DHT(Node myNode, int peerPort, DHTPeersReceiver peersReceiver){
        this.myNode = myNode;
        this.peerPort = peerPort;
        this.peersReceiver = peersReceiver;
        krpc = new KRPC(myNode, new QueryReceiver(this));
        responseReceiver = new ResponseReceiver(this);
        blacklist = new HashMap<>();
        outgoingTokens = new HashMap<>();
        peerQueries = new LinkedList<>();
    }

    public DHT(int port, int peerPort, DHTPeersReceiver peersReceiver){
        this(new Node(port), peerPort, peersReceiver);
    }

    public void start() throws SocketException {
        stopped.set(false);
        nodeList = new NodeList(myNode.getNodeId());
        peerList = new PeerList();
        krpc.start();
        thread = new Thread(this);
        thread.setName("DHT_Ticker");
        thread.start();
    }

    public void stop(){
        stopped.set(true);
        thread.interrupt();
        krpc.stop();
        nodeList.clear();
        nodeList = null;
        peerList.clear();
        peerList = null;
        synchronized (blacklist){
            blacklist.clear();
        }
        synchronized (outgoingTokens){
            outgoingTokens.clear();
        }
        synchronized (peerQueries){
            peerQueries.clear();
        }
    }

    @Override
    public void run() {
        while(!thread.isInterrupted()){

            if (Logger.verbose)
                Logger.log("Have " + nodeList.size() + " nodes in " +
                        nodeList.numOfBuckets() + " buckets.");

            krpc.tick();
            nodeList.tick(this);
            peerList.tick();
            tick();


            try {
                Thread.sleep(TICK_THREAD_SLEEP);
            } catch (InterruptedException ie) {
                break;
            }
        }
    }

    private void tick(){
        long now = System.currentTimeMillis();

        // Clean blacklist
        if (lastBlacklistClean < now - BLACKLIST_CLEAN_INTERVAL){
            synchronized (blacklist){
                List<ByteBuffer> removeItems = new LinkedList<>();
                for (Map.Entry<ByteBuffer, Long> entry : blacklist.entrySet()){
                    if (entry.getValue() < now - BLACKLIST_TIMEOUT)
                        removeItems.add(entry.getKey());
                }

                for (ByteBuffer id : removeItems)
                    blacklist.remove(id);

                if (Logger.verbose)
                    Logger.log("Clean blacklist: " + removeItems.size() + " out of " +
                            (blacklist.size() + removeItems.size()) + " items removed.");
            }
            lastBlacklistClean = now;
        }

        // Clean tokens
        if (lastTokensClean < now - TOKENS_CLEAN_INTERVAL){
            synchronized (outgoingTokens){
                List<ByteBuffer> removeItems = new LinkedList<>();
                for (Map.Entry<ByteBuffer, Token> entry : outgoingTokens.entrySet()){
                    if (entry.getValue().getGeneratedTime() < now - TOKEN_TIMEOUT)
                        removeItems.add(entry.getKey());
                }

                for (ByteBuffer token : removeItems)
                    outgoingTokens.remove(token);

                if (Logger.verbose)
                    Logger.log("Clean tokens: " + removeItems.size() + " out of " +
                            (outgoingTokens.size() + removeItems.size()) + " items removed.");
            }
            lastTokensClean = now;
        }

        // Announce
        if (lastAnnounce < now - ANNOUNCE_INTERVAL){
            synchronized (peerQueries){
                List<PeerQuery> removeItems = new LinkedList<>();
                for (PeerQuery peerQuery : peerQueries){
                    if (!peerQuery.shouldContinue()){
                        removeItems.add(peerQuery);
                        if (peerQuery.isAnnounce())
                            announce(peerQuery);
                    }
                }

                peerQueries.removeAll(removeItems);

                if (Logger.verbose)
                    Logger.log("Announcer: " + removeItems.size() + " out of " +
                            (peerQueries.size() + removeItems.size()) + " items removed.");
            }
            lastAnnounce = now;
        }

    }

    public void addToBlackList(ByteBuffer id){
        synchronized (blacklist){
            blacklist.put(id, System.currentTimeMillis());
        }
    }

    public void removeFromBlackList(ByteBuffer id){
        synchronized (blacklist){
            blacklist.remove(id);
        }
    }

    public boolean isInBlackList(ByteBuffer id){
        synchronized (blacklist){
            return blacklist.keySet().contains(id);
        }
    }

    public Node getMyNode(){
        return myNode;
    }

    public void getPeers(ByteBuffer info_hash, boolean announce){
        getPeers(info_hash, announce, DEFAULT_GETPEERS_DEPTH, DEFAULT_GETPEERS_MAXPEERS);
    }

    public void getPeers(ByteBuffer info_hash, boolean announce, int depth, int maxPeers){
        PeerQuery peerQuery = new PeerQuery(info_hash, maxPeers, announce, depth);

        synchronized (peerQueries){
            peerQueries.add(peerQuery);
        }

        List<Node> nodes = nodeList.findClosest(info_hash, 8);
        for (Node node : nodes)
            sendGetPeers(node, depth, peerQuery);
    }

    private void announce(PeerQuery peerQuery){
        Map<Node, Object> announceNodes = peerQuery.getAnnounceNodes();
        if (announceNodes == null || announceNodes.size() == 0)
            return;

        for(Map.Entry<Node, Object> entry : announceNodes.entrySet()){
            sendAnnouncePeer(entry.getKey(), peerQuery.getInfoHash(), peerPort, entry.getValue());
        }
    }

    private void sendQuery(Node node, String method, Map<ByteBuffer, Object> args, Object opaque){
        if (stopped.get())
            return;
        try {
            args.put(ByteBuffer.wrap("id".getBytes()), myNode.getNodeId());
            krpc.sendQuery(node, method, args, responseReceiver, opaque );
        } catch (Exception e) {
            e.printStackTrace();
        }

        //if (Logger.verbose)
        //    Logger.log("Sent query " + method + " to " + node.getIp().toString());
    }

    public void sendPing(Node node){
        sendQuery(node, "ping", new HashMap<ByteBuffer, Object>(), null);

        if (Logger.verbose)
            Logger.log("Sent query ping: " + node.getIp().toString());
    }

    private void sendFindNode(Node node, ByteBuffer target, int depth){
        Map<ByteBuffer, Object> args = new HashMap<>();
        args.put(ByteBuffer.wrap("target".getBytes()), target);
        sendQuery(node, "find_node", args, depth);

        if (Logger.verbose)
            Logger.log("Sent query find_node: Requested target " +
                    Logger.toHex(target.array()) + " from " + node.getIp().toString());
    }

    private void sendGetPeers(Node node, int depth, PeerQuery peerQuery){

        if (peerQuery.isAlreadyTried(node.getNodeId()))
            return;

        GetPeersOpaque opaque = new GetPeersOpaque();
        opaque.depth = depth;
        opaque.peerQuery = peerQuery;

        Map<ByteBuffer, Object> args = new HashMap<>();
        args.put(ByteBuffer.wrap("info_hash".getBytes()), peerQuery.getInfoHash());
        sendQuery(node, "get_peers", args, opaque);

        if (Logger.verbose)
            Logger.log("Sent query get_peers: Requested infohash " +
                    Logger.toHex(peerQuery.getInfoHash().array()) + " from " + node.getIp().toString());
    }

    private void sendAnnouncePeer(Node node, ByteBuffer info_hash, int port, Object token){
        Map<ByteBuffer, Object> args = new HashMap<>();
        args.put(ByteBuffer.wrap("info_hash".getBytes()), info_hash);
        args.put(ByteBuffer.wrap("port".getBytes()), (long) port);
        args.put(ByteBuffer.wrap("token".getBytes()), token);
        sendQuery(node, "announce_peer", args, null);

        if (Logger.verbose)
            Logger.log("Sent query announce_peer: Announced infohash " +
                    Logger.toHex(info_hash.array()) + " to " + node.getIp().toString());
    }

    public void explore(final List<Node> nodes, final ByteBuffer target, final int depth){
        Runnable explorerRunnable = new Runnable() {
            @Override
            public void run() {
                if (nodes.isEmpty())
                    return;

                for (Node node : nodes) {
                    sendFindNode(node, target, depth);
                    try {
                        // Give the closer nodes a chance to answer first
                        Thread.sleep(EXPLORER_NODE_INTERVAL);
                    } catch (InterruptedException e) {
                        //
                    }
                    if (stopped.get())
                        return;
                }
            }
        };

        Thread explorer = new Thread(explorerRunnable);
        explorer.setName("DHTExplorerThread");
        explorer.start();

    }

    public void explore(ByteBuffer target, int maxNodes, int depth){
        List<Node> nodes = nodeList.findClosest(target, maxNodes);
        explore(nodes, target, depth);
    }

    public void addNodes(List<Node> nodes){
        if (nodeList.size() <= NodeList.EXPLORE_MAX_NODES) {
            explore(nodes, myNode.getNodeId(), 2);
        } else {
            for (Node node : nodes)
                sendPing(node);
        }
    }

    public List<Node> addNodes(ByteBuffer nodes, int ipLength, boolean returnAll) throws UnknownHostException {
        List<Node> addedNodes = new LinkedList<>();
        int compactElementSize = 20 + ipLength + 2;
        if (nodes == null || nodes.array().length % compactElementSize != 0)
            return null;
        int numNodes = nodes.array().length / compactElementSize;
        for (int i=0; i<numNodes; i++) {
            byte[] newNodeIDBA = new byte[20];
            nodes.get(newNodeIDBA);
            byte[] newNodeIPBA = new byte[ipLength];
            nodes.get(newNodeIPBA);
            byte[] newNodePortBA = new byte[2];
            nodes.get(newNodePortBA);
            ByteBuffer newNodeID = ByteBuffer.wrap(newNodeIDBA);
            InetAddress newNodeIP = InetAddress.getByAddress(newNodeIPBA);
            int newNodePort = ((newNodePortBA[0] & 0xFF) << 8) | (newNodePortBA[1] & 0xFF);
            if (isInBlackList(newNodeID))
                continue;
            boolean alreadyIn = nodeList.get(newNodeID) != null;
            Node node = nodeList.putIfAbsent(newNodeID, newNodeIP, newNodePort, false, returnAll);
            if (node != null) {
                node.setLastSeen();
                if (!alreadyIn)
                    node.setQuestionable(true);
                if (returnAll || !alreadyIn)
                    addedNodes.add(node);
            }
        }
        return addedNodes;
    }

    private void heardFrom(ByteBuffer nodeID, InetAddress ip, int port, boolean isPermanent){
        if (nodeID.equals(myNode.getNodeId()))
            return;

        Node node = nodeList.putIfAbsent(nodeID, ip, port, isPermanent, false);
        if (node == null)
            return;
        node.setLastSeen();
        node.setQuestionable(false);
        removeFromBlackList(nodeID);

        // If get_peers response was low, we try to enhence it
        synchronized (peerQueries){
            for (PeerQuery peerQuery : peerQueries){
                if (peerQuery.shouldContinue() && peerQuery.getTriedNodesCount() < 8)
                    sendGetPeers(node, peerQuery.getOriginalDepth(), peerQuery);
            }
        }
    }

    private ByteBuffer generateCompactNodes(List<Node> nodeList){
        ByteBuffer nodes;
        if (nodeList != null && nodeList.size() > 0){
            nodes = ByteBuffer.allocate(nodeList.get(0).getCompactInfo().length * nodeList.size());
            for (Node node : nodeList)
                nodes.put(node.getCompactInfo());
        } else {
            nodes = ByteBuffer.allocate(0);
        }
        return nodes;
    }


    private static class ResponseReceiver implements KRPC.OnResponseReceived {

        private WeakReference<DHT> dhtWeakReference;

        public ResponseReceiver(DHT dht){
            dhtWeakReference = new WeakReference<DHT>(dht);
        }

        @Override
        public void onResponseReceived(KRPC.QueryData queryData, boolean error, Map response, List errorResponse) throws Exception {
            DHT dht = dhtWeakReference.get();
            if (dht == null)
                return;

            if (dht.stopped.get())
                return;

            if (error){
                if (Logger.verbose)
                    Logger.log("Got error to query " + queryData.method + " from " +
                            queryData.sentTo.getIp().toString() + ": " +
                            errorResponse.get(0) + " - " +
                            new String(((ByteBuffer)errorResponse.get(1)).array()));
                return;
            }

            ByteBuffer nodeID = (ByteBuffer) response.get(ByteBuffer.wrap("id".getBytes()));
            if (nodeID.array().length != 20)
                return;

            dht.heardFrom(nodeID, queryData.sentTo.getIp(), queryData.sentTo.getPort(),
                    queryData.sentTo.isPermanent());

            int ipLength = queryData.sentTo.getIp().getAddress().length;

            if (Logger.verbose)
                Logger.log("Got response " + queryData.method + " from " +
                        queryData.sentTo.getIp().toString());

            switch (queryData.method){
                case "ping":
                    break;

                case "find_node":
                    ByteBuffer nodes = (ByteBuffer) response.get(ByteBuffer.wrap("nodes".getBytes()));
                    List<Node> addedNodes = dht.addNodes(nodes, ipLength, false);
                    if (Logger.verbose && addedNodes != null)
                        Logger.log("find_node: Got " + addedNodes.size() + " nodes from " +
                                queryData.sentTo.getIp().toString());
                    int depth = (Integer) queryData.opaque - 1;
                    if (depth > 0 && addedNodes != null) {
                        ByteBuffer target = (ByteBuffer) queryData.args.get(ByteBuffer.wrap("target".getBytes()));
                        for (Node node : addedNodes) {
                            dht.sendFindNode(node, target, depth);
                        }
                    }

                    break;

                case "get_peers":
                    GetPeersOpaque peersOpaque = (GetPeersOpaque) queryData.opaque;
                    PeerQuery peerQuery = peersOpaque.peerQuery;
                    Object token = response.get(ByteBuffer.wrap("token".getBytes()));
                    peerQuery.addTriedNode(queryData.sentTo, token);
                    List values = (List) response.get(ByteBuffer.wrap("values".getBytes()));
                    ByteBuffer nodes1 = (ByteBuffer) response.get(ByteBuffer.wrap("nodes".getBytes()));
                    if (values != null) { // Got peers
                        List<PeerInfo> peerList = new ArrayList<>(values.size());
                        for (Object peer : values)
                            peerList.add(new PeerInfo((ByteBuffer) peer, ipLength));
                        peerList = peerQuery.addPeers(peerList);
                        if (dht.peersReceiver != null)
                            dht.peersReceiver.gotPeersFromDHT(peerQuery.getInfoHash(), peerList);
                        if (Logger.verbose)
                            Logger.log("get_peers: Got " + peerList.size() + " peers from " +
                                    queryData.sentTo.getIp().toString());

                    } else if (nodes1 != null){ // Got nodes
                        List<Node> addedNodes1 = dht.addNodes(nodes1, ipLength, true);
                        if (Logger.verbose && addedNodes1 != null)
                            Logger.log("get_peers: Got " + addedNodes1.size() + " nodes from " +
                                    queryData.sentTo.getIp().toString());
                        int depth1 = peersOpaque.depth -1;
                        if (depth1 > 0 && addedNodes1 != null && peerQuery.shouldContinue()) {
                            for (Node node : addedNodes1) {
                                dht.sendGetPeers(node, depth1, peerQuery);
                            }
                        }

                    }
                    break;

                case "announce_peer":
                    break;

                default:
            }
        }

        @Override
        public void onTimeout(KRPC.QueryData queryData) {
            DHT dht = dhtWeakReference.get();
            if (dht == null)
                return;

            if (dht.stopped.get())
                return;

            long now = System.currentTimeMillis();

            if (queryData.sentTo.getLastSeen() >  now - KRPC.DEFAULT_QUERY_TIMEOUT)
                return;

            if (Logger.verbose)
                Logger.log("Query " + queryData.method + " to " +
                        queryData.sentTo.getIp().toString() + " timeouted.");


            queryData.sentTo.setQuestionable(true);

            if (queryData.sentTo.getLastSeen() > now - NodeList.NODE_PING_TIME){
                // Give it couple of chances before it becomes quationable
                dht.sendPing(queryData.sentTo);
                queryData.sentTo.setLastSeen(now - NodeList.NODE_PING_TIME);
            }

            // Don't remove it just yet
            /*
            ByteBuffer nodeID = queryData.sentTo.getNodeId();


            dht.nodeList.remove(nodeID);

            if (!dht.isInBlackList(nodeID))
                dht.addToBlackList(nodeID);
                */
        }
    }


    private static class QueryReceiver implements KRPC.OnQueryReceived {

        private WeakReference<DHT> dhtWeakReference;

        public QueryReceiver(DHT dht){
            dhtWeakReference = new WeakReference<DHT>(dht);
        }

        @Override
        public Map onQueryReceived(ByteBuffer mID, InetAddress ip, int port, String method, Map args) throws Exception{
            DHT dht = dhtWeakReference.get();
            if (dht == null)
                return null;

            if (dht.stopped.get())
                return null;

            ByteBuffer nodeID = (ByteBuffer) args.get(ByteBuffer.wrap("id".getBytes()));
            if (nodeID.array().length != 20)
                return null;

            dht.heardFrom(nodeID, ip, port, false);

            Map<ByteBuffer, Object> response = new HashMap<>();
            response.put(ByteBuffer.wrap("id".getBytes()), dht.myNode.getNodeId());

            if (Logger.verbose)
                Logger.log("Recieved query " + method + " from " + ip.toString());


            switch (method){
                case "ping":
                    break;

                case "find_node":
                    ByteBuffer target = (ByteBuffer) args.get(ByteBuffer.wrap("target".getBytes()));
                    List<Node> nodeList = dht.nodeList.findClosest(target, NUM_RETURN_NODES);
                    ByteBuffer nodes = dht.generateCompactNodes(nodeList);
                    response.put(ByteBuffer.wrap("nodes".getBytes()), nodes);
                    if (Logger.verbose && nodeList != null)
                        Logger.log("find_node: Sent " + nodeList.size() + " nodes to " +
                                ip.toString());


                    break;

                case "get_peers":
                    // Generate token
                    Token token = new Token(nodeID);
                    response.put(ByteBuffer.wrap("token".getBytes()), token.getToken());
                    synchronized (dht.outgoingTokens){
                        dht.outgoingTokens.put(token.getToken(), token);
                    }
                    // Get peers
                    ByteBuffer info_hash = (ByteBuffer) args.get(ByteBuffer.wrap("info_hash".getBytes()));
                    List<PeerInfo> peers = dht.peerList.getPeers(info_hash);

                    if (Logger.verbose)
                        Logger.log("get_peers: node " + ip.toString() + " requested infohash " +
                                Logger.toHex(info_hash.array()));


                    if (peers != null && peers.size()>0){
                        // Send peers
                        List<ByteBuffer> values = new ArrayList<>(peers.size());
                        for (PeerInfo peer : peers)
                            values.add(ByteBuffer.wrap(peer.getCompactInfo()));
                        response.put(ByteBuffer.wrap("values".getBytes()), values);

                        if (Logger.verbose)
                            Logger.log("get_peers: Sent " + values.size() + " peers to " +
                                    ip.toString());
                    } else {
                        // Send nodes
                        List<Node> nodeList1 = dht.nodeList.findClosest(info_hash, NUM_RETURN_NODES);
                        ByteBuffer nodes1 = dht.generateCompactNodes(nodeList1);
                        response.put(ByteBuffer.wrap("nodes".getBytes()), nodes1);
                        if (Logger.verbose && nodeList1 != null)
                            Logger.log("get_peers: Sent " + nodeList1.size() + " nodes to " +
                                    ip.toString());

                    }
                    break;

                case "announce_peer":
                    // Verify token
                    ByteBuffer token1 = (ByteBuffer) args.get(ByteBuffer.wrap("token".getBytes()));
                    synchronized (dht.outgoingTokens){
                        Token outToken = dht.outgoingTokens.get(token1);
                        if (outToken == null || !outToken.getNodeID().equals(nodeID))
                            return null;
                    }

                    ByteBuffer info_hash1 = (ByteBuffer) args.get(ByteBuffer.wrap("info_hash".getBytes()));
                    Long port1 = (Long) args.get(ByteBuffer.wrap("port".getBytes()));
                    Long implied_port = (Long) args.get(ByteBuffer.wrap("implied_port".getBytes()));
                    if (implied_port != null && implied_port != 0)
                        port1 = (long) port;

                    dht.peerList.announce(info_hash1, new PeerInfo(ip, port1.intValue()));
                    if (Logger.verbose)
                        Logger.log("announce peer: " + ip.toString() + " with infohash " +
                                Logger.toHex(info_hash1.array()));

                    break;

                default:
                    return null;
            }


            return response;
        }
    }
}
