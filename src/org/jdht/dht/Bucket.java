/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class Bucket {

    private int rangeBegin;
    private int rangeEnd;
    private Set<ByteBuffer> entries;
    private int maxEntries;
    private long lastChanged = 0;

    public Bucket(int begin, int end, int max) {
        rangeBegin = begin;
        rangeEnd = end;
        maxEntries = max;
        entries = new HashSet<>();
    }

    public Set<ByteBuffer> getEntries() {
        return Collections.unmodifiableSet(entries);
    }

    public boolean add(ByteBuffer entry) {
        if (entries.size() >= 2*maxEntries)
            return false;
        boolean added = entries.add(entry);
        setLastChanged();
        return added;
    }

    public boolean remove(ByteBuffer entry){
        return entries.remove(entry);
    }

    public void clear() {
        entries.clear();
    }

    public boolean isFull(){
        return entries.size() >= maxEntries;
    }

    public int getRangeBegin() {
        return rangeBegin;
    }

    public int getRangeEnd() {
        return rangeEnd;
    }

    public int size(){
        return entries.size();
    }

    public long getLastChanged() {
        return lastChanged;
    }

    public void setLastChanged() {
        lastChanged = System.currentTimeMillis();
    }
}