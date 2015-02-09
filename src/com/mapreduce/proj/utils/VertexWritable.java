package com.mapreduce.proj.utils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class VertexWritable implements Writable, Cloneable {

    private LongWritable vertexId;
    private TreeSet<LongWritable> adjacencies;
    private boolean activated;

    public VertexWritable() {
        super();
    }

    public VertexWritable(LongWritable minimalVertexId) {
        super();
        this.vertexId = minimalVertexId;
    }

    // true if updated
    public boolean setMinimalVertex(LongWritable id) {
        if (vertexId == null) {
            vertexId = id;
            return true;
        } else {
            if (id.get() < vertexId.get()) {
                vertexId = id;
                return true;
            }
        }
        return false;
    }

    public boolean isMessage() {
        return adjacencies == null;
    }

    public VertexWritable makeMessage() {
        return new VertexWritable(vertexId);
    }

    public void addAdjacency(LongWritable id) {
        if (adjacencies == null)
            adjacencies = new TreeSet<LongWritable>();
        adjacencies.add(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        if (adjacencies == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(adjacencies.size());
            for (LongWritable l : adjacencies) {
                l.write(out);
            }
        }
        out.writeBoolean(activated);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId = new LongWritable();
        vertexId.readFields(in);
        int length = in.readInt();
        if (length > -1) {
            adjacencies = new TreeSet<LongWritable>();
            for (int i = 0; i < length; i++) {
                LongWritable temp = new LongWritable();
                temp.readFields(in);
                adjacencies.add(temp);
            }
        } else {
            adjacencies = null;
        }
        activated = in.readBoolean();
    }

    @Override
    public String toString() {
        return  vertexId + "\t" + adjacencies;
    }

    @Override
    public VertexWritable clone(){
        VertexWritable toReturn = new VertexWritable(new LongWritable(
                vertexId.get()));
        if (adjacencies != null) {
            toReturn.adjacencies = new TreeSet<LongWritable>();
            for (LongWritable l : adjacencies) {
                toReturn.adjacencies.add(new LongWritable(l.get()));
            }
        }
        return toReturn;
    }

    public boolean isActivated() {
        return activated;
    }

    public void setVertexId(LongWritable vertexId) {
        this.vertexId = vertexId;
    }

    public void setAdjacencies(TreeSet<LongWritable> adjacencies) {
        this.adjacencies = adjacencies;
    }

    public void setActivated(boolean activated) {
        this.activated = activated;
    }

    public LongWritable getVertexId() {
        return vertexId;
    }

    public TreeSet<LongWritable> getAdjacencies() {
        return adjacencies;
    }

}
