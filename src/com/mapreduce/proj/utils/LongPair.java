package com.mapreduce.proj.utils;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Ray on 3/31/14.
 * Project: MapReduceFinal
 */
public class LongPair implements WritableComparable<LongPair> {
    private long first;
    private long second;

    public LongPair(){}

    public LongPair(long first, long second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
    }

    @Override
    public int compareTo(LongPair longPair) {
        if (first != longPair.first) {
            return first < longPair.first ? -1 : 1;
        } else if (second != longPair.second) {
            return second < longPair.second ? -1 : 1;
        } else {
            return 0;
        }
    }

    // getters
    public long getFirst() {
        return this.first;
    }
    public long getSecond() {
        return this.second;
    }
}
