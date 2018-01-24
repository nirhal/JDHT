/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

public class IDGenerator {

    private final static BigInteger allOnes;
    private final static BigInteger one = new BigInteger("1");

    static {
        byte[] allOnesByteArray = new byte[20];
        for (int i=0; i<20; i++)
            allOnesByteArray[i] = (byte) 0xff;
        allOnes = new BigInteger(1, allOnesByteArray);
    }

    public static ByteBuffer generateRandomID(){
        byte[] nid = new byte[20];
        new Random().nextBytes(nid);
        // try to do SHA1 to generate more entropy
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            nid = md.digest(nid);
        } catch (NoSuchAlgorithmException ex) {
            //
        }
        return ByteBuffer.wrap(nid);
    }

    public static ByteBuffer generateRandomID(BigInteger myID, int range){
        byte[] b = new byte[20];
        new Random().nextBytes(b);
        BigInteger b_bi = new BigInteger(1, b);

        // create masks
        BigInteger mask_a = allOnes.shiftRight(160-range);
        BigInteger mask_b = mask_a.xor(allOnes);

        BigInteger newval = mask_a.and(b_bi).or(mask_b.and(myID)); // compose two parts
        newval = newval.xor(new BigInteger("1").shiftLeft(range)); // invert 'range' bit

        // copy array
        byte[] newvalArray = newval.toByteArray();
        byte[] ret = new byte[20];
        Arrays.fill( ret, (byte) 0 );
        for (int i=newvalArray.length-1, j= 19; i>=0 && j>=0; i--, j--)
            ret[j] = newvalArray[i];

        return ByteBuffer.wrap(ret);
    }

    public static ByteBuffer generateRandomID(ByteBuffer myID, int range){
        return generateRandomID(new BigInteger(1, myID.array()), range);
    }
}
