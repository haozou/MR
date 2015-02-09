package com.mapreduce.proj.adjlist;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UndirectedEdgeMapper extends
        Mapper<Object, Text, LongWritable, Text> {

    private HashMap<Long, ArrayList<String>> vertexMap;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        vertexMap = new HashMap<Long, ArrayList<String>>();
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<Long, ArrayList<String>> entry : vertexMap.entrySet()) {
            StringBuilder sb = new StringBuilder();
            for (String vertex : entry.getValue()) {
                sb.append(vertex);
                sb.append("\t");
            }
            context.write(new LongWritable(entry.getKey()), new Text(sb.toString()));
        }
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] edge = value.toString().split(",");
        long srcVertex = Integer.valueOf(edge[0]);
        String tgtVertex = edge[1];

        // one direction of the undirected edge
        if (vertexMap.containsKey(srcVertex)) {
            vertexMap.get(srcVertex).add(tgtVertex);
        } else {
            ArrayList<String> tgtVertices = new ArrayList<String>();
            tgtVertices.add(tgtVertex);
            vertexMap.put(srcVertex, tgtVertices);
        }

        // the other direction of the undirected edge
        srcVertex = Long.valueOf(edge[1]);
        tgtVertex = edge[0];
        if (vertexMap.containsKey(srcVertex)) {
            vertexMap.get(srcVertex).add(tgtVertex);
        } else {
            ArrayList<String> tgtVertices = new ArrayList<String>();
            tgtVertices.add(tgtVertex);
            vertexMap.put(srcVertex, tgtVertices);
        }
    }


}
