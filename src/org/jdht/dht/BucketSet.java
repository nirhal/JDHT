/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.jdht.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class BucketSet {

    private static final int BUCKET_SIZE = 8;
    private static final int ID_SIZE = 160;

    public static interface Trimmer {
        public boolean trim(Bucket b);
    }

    private List<Bucket> buckets;
    private ByteBuffer myID;
    private BigInteger myIDInt;
    private Trimmer trimmer;

    public BucketSet(ByteBuffer myID, Trimmer trimmer){
        this.myID = myID;
        this.myIDInt = new BigInteger(1, myID.array());
        this.trimmer = trimmer;
        buckets = new ArrayList<Bucket>(4);
        buckets.add(new Bucket(0, ID_SIZE-1, BUCKET_SIZE));
    }


    public int size() {
        int count = 0;
        for (Bucket b : buckets) {
            count += b.size();
        }
        return  count;
    }

    public boolean add(ByteBuffer id) {
        Bucket b = getBucket(id);
        if (b == null)
            return false;
        if (b.getRangeBegin() == b.getRangeEnd() && b.isFull() && !trimmer.trim(b))
            return false;
        if (b.add(id)) {
            if (shouldSplit(b)){
                return split(b.getRangeBegin(), id);
            }
            return true;
        }
        return false;
    }

    public boolean remove(ByteBuffer id) {
        Bucket b = getBucket(id);
        return b != null && b.remove(id);
    }

    public void clear() {
        for (Bucket b : buckets)
            b.clear();
        buckets.clear();
        buckets.add(new Bucket(0, ID_SIZE-1, BUCKET_SIZE));
    }

    public List<Bucket> getBuckets(){
        return Collections.unmodifiableList(buckets);
    }

    public List<ByteBuffer> getClosest(int max) {
        List<ByteBuffer> closest = new ArrayList<>(max);
        int count = 0;

        // start at first (closest) bucket
        for (int i = 0; i < buckets.size() && count < max; i++) {
            Set<ByteBuffer> entries = buckets.get(i).getEntries();
            // add the whole bucket,
            // extras will be trimmed after sorting
            for (ByteBuffer e : entries) {
                closest.add(e);
                count++;
            }
        }

        XORComparator comp = new XORComparator(myID);
        Collections.sort(closest, comp);
        int sz = closest.size();
        for (int i = sz - 1; i >= max; i--) {
            closest.remove(i);
        }
        return closest;
    }

    public List<ByteBuffer> getClosest(ByteBuffer id, int max) {
        if (id.equals(myID))
            return getClosest(max);
        List<ByteBuffer> closest = new ArrayList<>(max);
        int count = 0;
        int start = getBucketIndex(id);
        // start at closest bucket, then to the smaller (closer to us) buckets
        for (int i = start; i >= 0 && count < max; i--) {
            Set<ByteBuffer> entries = buckets.get(i).getEntries();
            for (ByteBuffer e : entries) {
                closest.add(e);
                count++;
            }
        }
        // then the farther from us buckets if necessary
        for (int i = start + 1; i < buckets.size() && count < max; i++) {
            Set<ByteBuffer> entries = buckets.get(i).getEntries();
            for (ByteBuffer e : entries) {
                closest.add(e);
                count++;
            }
        }
        XORComparator comp = new XORComparator(id);
        Collections.sort(closest, comp);
        int sz = closest.size();
        for (int i = sz - 1; i >= max; i--) {
            closest.remove(i);
        }
        return closest;
    }

    private boolean split(int range, ByteBuffer added){
        int bucketIdx = getBucketIndex(range);
        while (shouldSplit(buckets.get(bucketIdx))) {
            Bucket b = buckets.get(bucketIdx);

            Bucket b1 = new Bucket(b.getRangeBegin(), b.getRangeEnd()-1, BUCKET_SIZE);
            Bucket b2 = new Bucket(b.getRangeEnd(), b.getRangeEnd(), BUCKET_SIZE);
            for (ByteBuffer id : b.getEntries()) {
                if (getRange(id) < b2.getRangeBegin())
                    b1.add(id);
                else
                    b2.add(id);
            }
            buckets.set(bucketIdx, b1);
            buckets.add(bucketIdx + 1, b2);

            if (b2.size() > BUCKET_SIZE){
                b2.remove(added);
                if (trimmer.trim(b)) {
                    b2.add(added);
                } else {
                    return false;
                }
            }
        }
        return true;
    }


    private boolean shouldSplit(Bucket b) {
        return b.getRangeBegin() != b.getRangeEnd() && b.size() > BUCKET_SIZE;
    }


    private Bucket getBucket(ByteBuffer id) {
        int bucketIdx = getBucketIndex(id);
        if (bucketIdx < 0)
            return null;
        return buckets.get(bucketIdx);
    }

    private int getBucketIndex(int range) {
            for (int i = buckets.size() - 1; i >= 0; i--) {
                Bucket b = buckets.get(i);
                if (range >= b.getRangeBegin() && range <= b.getRangeEnd())
                    return i;
            }
            return -1;
    }

    private int getBucketIndex(ByteBuffer id) {
        int range = getRange(id);
        if (range < 0)
            return -1;
        int bucketIdx = getBucketIndex(range);
        if (bucketIdx < 0)
            throw new IllegalStateException("ID does not fit in any bucket");
        return bucketIdx;

    }

    private int getRange(ByteBuffer id){
        BigInteger compRes = myIDInt.xor(new BigInteger(1, id.array()));
        return compRes.bitLength() - 1;
    }

    public static class XORComparator implements Comparator<ByteBuffer> {
        private final byte[] target;

        public XORComparator(ByteBuffer target) {
            this.target = target.array();
        }

        public int compare(ByteBuffer a, ByteBuffer b) {
            byte a_ba[] = a.array();
            byte b_ba[] = b.array();
            for (int i = 0; i < target.length; i++) {
                int ld = (a_ba[i] ^ target[i]) & 0xff;
                int rd = (b_ba[i] ^ target[i]) & 0xff;
                if (ld < rd)
                    return -1;
                if (ld > rd)
                    return 1;
            }
            return 0;
        }
    }
}
