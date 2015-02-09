package com.mapreduce.proj.utils;

import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

/**
 * Created by Ray on 4/16/14.
 * Project: MapReduceFinal
 */
public class VertexWithDistanceWritable extends VertexWritable {

    private LongWritable distance;
    private LongWritable preVertexId;

    public VertexWithDistanceWritable(){
        super();
    }

    public VertexWithDistanceWritable(LongWritable vertexId){
        super(vertexId);
    }

    public VertexWithDistanceWritable makeMessage(){
        return new VertexWithDistanceWritable(super.getVertexId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.getVertexId().write(out);
        distance.write(out);
        preVertexId.write(out);
        if (super.getAdjacencies() == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(super.getAdjacencies().size());
            for (LongWritable l : super.getAdjacencies()) {
                l.write(out);
            }
        }
        out.writeBoolean(super.isActivated());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.setVertexId(new LongWritable());
        super.getVertexId().readFields(in);
        distance = new LongWritable();
        distance.readFields(in);
        preVertexId = new LongWritable();
        preVertexId.readFields(in);
        int length = in.readInt();
        if (length > -1) {
            super.setAdjacencies(new TreeSet<LongWritable>());
            for (int i = 0; i < length; i++) {
                LongWritable temp = new LongWritable();
                temp.readFields(in);
                super.getAdjacencies().add(temp);
            }
        } else {
            super.setAdjacencies(null);
        }
        super.setActivated(in.readBoolean());
    }

    @Override
    public String toString() {
        return distance + "\t" + super.getAdjacencies();
    }

    @Override
    public VertexWithDistanceWritable clone(){
        VertexWithDistanceWritable copy = new VertexWithDistanceWritable(new LongWritable(
                super.getVertexId().get()));
        copy.setDistance(distance);
        copy.setPreVertexId(preVertexId);
        if (super.getAdjacencies() != null) {
            copy.setAdjacencies(new TreeSet<LongWritable>());
            for (LongWritable l : super.getAdjacencies()) {
                copy.getAdjacencies().add(new LongWritable(l.get()));
            }
        }
        return copy;
    }

    public void setDistance(LongWritable distance) {
        this.distance = distance;
    }
    public LongWritable getDistance() {
        return distance;
    }

    public LongWritable getPreVertexId() {
        return preVertexId;
    }

    public void setPreVertexId(LongWritable preVertexId) {
        this.preVertexId = preVertexId;
    }


}
