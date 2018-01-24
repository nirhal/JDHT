/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.bencode;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class DictionaryComparator implements Comparator<ByteBuffer> {

    public DictionaryComparator() {
    }

    public int bitCompare(byte b1, byte b2) {
        int int1 = b1 & 0xFF;
        int int2 = b2 & 0xFF;
        return int1 - int2;
    }

    public int compare(ByteBuffer o1, ByteBuffer o2) {
        byte[] byteString1 = o1.array();
        byte[] byteString2 = o2.array();
        int minLength = byteString1.length > byteString2.length ? byteString2.length : byteString1.length;
        for (int i = 0; i < minLength; i++) {
            int bitCompare = bitCompare(byteString1[i], byteString2[i]);
            if (bitCompare != 0) {
                return bitCompare;
            }
        }

        if (byteString1.length > byteString2.length) {
            return 1;
        } else if (byteString1.length < byteString2.length) {
            return -1;
        }
        return 0;
    }
}
