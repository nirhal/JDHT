/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.bencode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Bencode {

    private Object rootElement = null;

    /**
     * This creates and parse a bencoded InputStream
     */
    public Bencode(InputStream is) throws IOException {
        if (!is.markSupported()) {
            throw new IOException("is.markSupported should be true");
        }
        rootElement = parse(is);
    }

    /**
     * This creates a new instance of Bencode class
     */
    public Bencode() {
    }

    /**
     * This method prints the bencoded file on the OutputStream os
     */
    public void print(OutputStream os) throws IOException {
        print(rootElement, os);
    }

    private void print(Object object, OutputStream os) throws IOException {
        if (object instanceof Long) {
            os.write('i');
            os.write(((Long) object).toString().getBytes());
            os.write('e');
        }
        if (object instanceof ByteBuffer) {
            byte[] byteString = ((ByteBuffer) object).array();
            os.write(Integer.toString(byteString.length).getBytes());
            os.write(':');
            os.write(byteString);
            //for (int i = 0; i < byteString.length; i++) {
            //    os.write(byteString[i]);
            //}
        } else if (object instanceof List) {
            List list = (List) object;
            os.write('l');
            for (Object elem : list) {
                print(elem, os);
            }
            os.write('e');
        } else if (object instanceof Map) {
            Map map = (Map) object;
            os.write('d');

            SortedMap<ByteBuffer, Object> sortedMap = new TreeMap<ByteBuffer, Object>(new DictionaryComparator());
            // sortedMap.putAll(map);

            for (Object elem : map.entrySet()) {
                Map.Entry entry = (Map.Entry) elem;
                sortedMap.put((ByteBuffer) entry.getKey(), entry.getValue());
            }

            for (Object elem : sortedMap.entrySet()) {
                Map.Entry entry = (Map.Entry) elem;
                print(entry.getKey(), os);
                print(entry.getValue(), os);
            }
            os.write('e');
        }
    }
    
    public static int getBencodeSize(Object object) {
    	int size = 0;
        if (object instanceof Long) {
            size = 2 + ((Long) object).toString().getBytes().length;
        }
        if (object instanceof ByteBuffer) {
            byte[] byteString = ((ByteBuffer) object).array();
            size = Integer.toString(byteString.length).getBytes().length + 1 + byteString.length;
        } else if (object instanceof List) {
            List list = (List) object;
            size = 2;
            for (Object elem : list) {
                size+=getBencodeSize(elem);
            }
        } else if (object instanceof Map) {
            Map map = (Map) object;
            size = 2;

            SortedMap<ByteBuffer, Object> sortedMap = new TreeMap<ByteBuffer, Object>(new DictionaryComparator());
            // sortedMap.putAll(map);

            for (Object elem : map.entrySet()) {
                Map.Entry entry = (Map.Entry) elem;
                sortedMap.put((ByteBuffer) entry.getKey(), entry.getValue());
            }

            for (Object elem : sortedMap.entrySet()) {
                Map.Entry entry = (Map.Entry) elem;
                size+=getBencodeSize(entry.getKey());
                size+=getBencodeSize(entry.getValue());
            }
        }
        return size;
    }

    private Object parse(InputStream is) throws IOException {
        is.mark(0);
        int readChar = is.read();
        switch (readChar) {
            case 'i':
                return parseInteger(is);
            case 'l':
                return parseList(is);
            case 'd':
                return parseDictionary(is);
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                is.reset();
                return parseByteString(is);
            default:
                throw new IOException("Problem parsing bencoded file");
        }
    }

    public Object getRootElement() {
        return rootElement;
    }

    public void setRootElement(Object rootElement) {
        this.rootElement = rootElement;
    }

    private Long parseInteger(InputStream is) throws IOException {

        int readChar = is.read();

        StringBuffer buff = new StringBuffer();
        do {
            if (readChar < 0) {
                throw new IOException("Unexpected EOF found");
            }
            buff.append((char) readChar);
            readChar = is.read();
        } while (readChar != 'e');

        return Long.parseLong(buff.toString());
    }

    private List<Object> parseList(InputStream is) throws IOException {

        List<Object> list = new LinkedList<Object>();
        is.mark(0);
        int readChar = is.read();
        while (readChar != 'e') {
            if (readChar < 0) {
                throw new IOException("Unexpected EOF found");
            }
            is.reset();
            list.add(parse(is));
            is.mark(0);
            readChar = is.read();
        }

        return list;
    }

    private SortedMap parseDictionary(InputStream is) throws IOException {
        SortedMap<ByteBuffer, Object> map = new TreeMap<ByteBuffer, Object>(new DictionaryComparator());
        is.mark(0);
        int readChar = is.read();
        while (readChar != 'e') {
            if (readChar < 0) {
                throw new IOException("Unexpected EOF found");
            }
            is.reset();
            map.put(parseByteString(is), parse(is));
            is.mark(0);
            readChar = is.read();
        }

        return map;
    }

    private ByteBuffer parseByteString(InputStream is) throws IOException {

        int readChar = is.read();

        StringBuffer buff = new StringBuffer();
        do {
            if (readChar < 0) {
                throw new IOException("Unexpected EOF found");
            }
            buff.append((char) readChar);
            readChar = is.read();
        } while (readChar != ':');
        Integer length = Integer.parseInt(buff.toString());

        byte[] byteString = new byte[length];
        for (int i = 0; i < byteString.length; i++) {
            byteString[i] = (byte) is.read();
        }
        return ByteBuffer.wrap(byteString);
    }
}
