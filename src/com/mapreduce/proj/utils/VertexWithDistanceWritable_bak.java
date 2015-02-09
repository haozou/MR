package com.mapreduce.proj.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

public class VertexWithDistanceWritable_bak implements Writable, Cloneable {

    private LongWritable vertexId;
    private LongWritable distance;
    private LongWritable preVertexId;
    private TreeSet<LongWritable> edges;
    private boolean activated;

    public VertexWithDistanceWritable_bak() {
        super();
    }

    public VertexWithDistanceWritable_bak(LongWritable vertexId) {
        super();
        this.vertexId = vertexId;
    }

    
    public boolean isMessage() {
        return edges == null;
    }

    public VertexWithDistanceWritable_bak makeMessage() {
        return new VertexWithDistanceWritable_bak(vertexId);
    }

    public void addVertex(LongWritable id) {
        if (edges == null)
            edges = new TreeSet<LongWritable>();
        edges.add(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        distance.write(out);
        preVertexId.write(out);
        if (edges == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(edges.size());
            for (LongWritable l : edges) {
                l.write(out);
            }
        }
        out.writeBoolean(activated);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId = new LongWritable();
        vertexId.readFields(in);
        distance = new LongWritable();
        distance.readFields(in);
        preVertexId = new LongWritable();
        preVertexId.readFields(in);
        int length = in.readInt();
        if (length > -1) {
            edges = new TreeSet<LongWritable>();
            for (int i = 0; i < length; i++) {
                LongWritable temp = new LongWritable();
                temp.readFields(in);
                edges.add(temp);
            }
        } else {
            edges = null;
        }
        activated = in.readBoolean();
    }

    @Override
    public String toString() {
        return distance + "\t" + edges;
    }

    public VertexWithDistanceWritable_bak clone(){
        VertexWithDistanceWritable_bak toReturn = new VertexWithDistanceWritable_bak(new LongWritable(
                vertexId.get()));
        toReturn.setDistance(distance);
        toReturn.setPreVertexId(preVertexId);
        if (edges != null) {
            toReturn.edges = new TreeSet<LongWritable>();
            for (LongWritable l : edges) {
                toReturn.edges.add(new LongWritable(l.get()));
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

    public void setEdges(TreeSet<LongWritable> edges) {
        this.edges = edges;
    }

    public void setActivated(boolean activated) {
        this.activated = activated;
    }

    public LongWritable getVertexId() {
        return vertexId;
    }

    public TreeSet<LongWritable> getEdges() {
        return edges;
    }
    public void setDistance(LongWritable distance) {
    	this.distance = distance;
    }
    public LongWritable getDistance() {
    	return distance;
    }

	public LongWritable getPreVertexId() {
		// TODO Auto-generated method stub
		return preVertexId;
	}

	public void setPreVertexId(LongWritable preVertexId) {
		// TODO Auto-generated method stub
		this.preVertexId = preVertexId;
	}

}
