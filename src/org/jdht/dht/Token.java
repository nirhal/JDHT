/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;

import java.nio.ByteBuffer;
import java.util.Random;

public class Token {

    private final static int TOKEN_LENGTH = 8;

    private ByteBuffer token;
    private ByteBuffer nodeID;
    private long generatedTime;

    public Token(ByteBuffer nodeID){
        byte[] tokenBA = new byte[TOKEN_LENGTH];
        new Random().nextBytes(tokenBA);
        token = ByteBuffer.wrap(tokenBA);
        this.nodeID = nodeID;
        generatedTime = System.currentTimeMillis();
    }

    public ByteBuffer getToken() {
        return token;
    }

    public ByteBuffer getNodeID() {
        return nodeID;
    }

    public long getGeneratedTime() {
        return generatedTime;
    }

}
