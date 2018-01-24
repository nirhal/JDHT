/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;

//import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

    // Logger class
    // Here I used simple print logger, but you can replace it with whatever you want.

    public static boolean verbose = true;

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    //private static org.slf4j.Logger logger = LoggerFactory.getLogger(DHT.class);
    private static DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void log(String s){
        //logger.debug(s);
        String time = LocalDateTime.now().format(formatter);
        System.out.println(time + ": " + s);
    }

    public static String toHex(byte[] bytes){
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

}
