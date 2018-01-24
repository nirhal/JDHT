/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.jdht.example;

import org.jdht.dht.*;


import java.io.File;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        try {

            DHTPeersReceiver peersReceiver = new DHTPeersReceiver() {
                @Override
                public void gotPeersFromDHT(ByteBuffer infoHash, List<PeerInfo> peers) {
                    for (PeerInfo peer : peers)
                        Logger.log("GOT PEER " + peer.getIp() + " FOR INFOHASH " + Logger.toHex(infoHash.array()));
                }
            };

            // Define DHT file
            File saveFile = new File(System.getProperty("user.home"), "dht.dat");

            // You can try come of these constructors

            //byte[] nodeID = hexStringToByteArray("653DBD6A8632865B26AF8FC9861339C49C03F4F6");
            //DHT dht = new DHT(new Node(ByteBuffer.wrap(nodeID), 5739), 5643, peersReceiver);

            //DHT dht = new DHT(5739, 5643, peersReceiver);

            DHT dht = new DHT(saveFile, 5739, 5643, peersReceiver);


            dht.start();

            List<Node> list = new LinkedList<>();
            list.add(new Node( InetAddress.getByName("router.bittorrent.com"), 6881, true));
            list.add(new Node( InetAddress.getByName("dht.transmissionbt.com"), 6881, true));
            list.add(new Node( InetAddress.getByName("router.utorrent.com"), 6881, true));
            dht.addNodes(list);


            byte[] infohash = hexStringToByteArray("653DBD6A8632865B26AF8FC9861339C49C034775");

            dht.getPeers(ByteBuffer.wrap(infohash), false);

            // Press ENTER to quit
            Scanner scan = new Scanner (System.in);
            scan.nextLine();

            dht.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len/2];

        for(int i = 0; i < len; i+=2){
            data[i/2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i+1), 16));
        }

        return data;
    }
}
