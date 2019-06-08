package student.dijkstra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Dijkstra {
    private final List<Vertex> vertices;
    private final List<Edge> edges;
    private Set<Vertex> settledNodes;
    private Set<Vertex> unSettledNodes;
    private Map<Vertex, Vertex> predecessors;
    private Map<Vertex, Integer> distance;

    public Dijkstra(Graph graph) {
        this.vertices = new ArrayList<>(graph.vertices);
        this.edges = new ArrayList<>(graph.edges);
    }

    public Pair<LinkedList<Vertex>, Integer> getPath(Vertex source, Vertex target) {
        if (vertices == null || edges == null) throw new RuntimeException("Graph is not initialized.");
        this.execute(source);
        LinkedList<Vertex> path = new LinkedList<>();
        Vertex current = target;
        if (predecessors.get(current) == null) return null;
        path.add(current);
        while (predecessors.get(current) != null) {
            current = predecessors.get(current);
            path.add(current);
        }
        //Collections.reverse(path);
        return new Pair(path, distance.get(target));
    }

    public Map<Vertex, Integer> getDistances(Vertex source) {
        if (vertices == null || edges == null) throw new RuntimeException("Graph is not initialized.");
        this.execute(source);
        LinkedList<Vertex> path = new LinkedList<>();
        return distance;
    }


    private void execute(Vertex source) {
        settledNodes = new HashSet<>();
        unSettledNodes = new HashSet<>();
        distance = new HashMap<>();
        predecessors = new HashMap<>();
        distance.put(source, 0);
        unSettledNodes.add(source);
        while (unSettledNodes.size() > 0) {
            Vertex node = getMinimum(unSettledNodes);
            settledNodes.add(node);
            unSettledNodes.remove(node);
            findMinimalDistances(node);
        }
    }

    private void findMinimalDistances(Vertex src) {
        List<Vertex> unsettledNeighbours = getUnsettledNeighbours(src);
        for (Vertex dst : unsettledNeighbours) {
            if (getShortestDistance(dst) > getShortestDistance(src) + getDistance(src, dst)) {
                distance.put(dst, getShortestDistance(src) + getDistance(src, dst));
                predecessors.put(dst, src);
                unSettledNodes.add(dst);
            }
        }
    }

    private int getDistance(Vertex src, Vertex dst) {
        for (Edge edge : edges) {
            if (edge.source.equals(src) && edge.destination.equals(dst)) {
                return edge.weight;
            }
        }
        return Integer.MAX_VALUE;
    }

    private List<Vertex> getUnsettledNeighbours(Vertex vertex) {
        List<Vertex> unsettledNeighbours = new ArrayList<>();
        for (Vertex v : vertex.neighbours){
            if(!settledNodes.contains(v)) unsettledNeighbours.add(v);
        }
        return unsettledNeighbours;
    }

    private Vertex getMinimum(Set<Vertex> vertices) {
        Vertex minimum = null;
        for (Vertex vertex : vertices) {
            if (minimum == null)  minimum = vertex;
            else if (getShortestDistance(vertex) < getShortestDistance(minimum)) {
                minimum = vertex;
            }
        }
        return minimum;
    }

    private int getShortestDistance(Vertex dst) {
        return distance.containsKey(dst) ? distance.get(dst) : Integer.MAX_VALUE;
    }

    public static void printSolution(Pair<LinkedList<Vertex>, Integer> path){
        StringBuilder sb = new StringBuilder("Path: " + System.lineSeparator());
        for (Vertex vertex : path.first) {
            sb.append(vertex.id + " -> ");
        }
        sb.delete(sb.length()-3, sb.length());
        sb.append(System.lineSeparator() + "Distance: " + path.second + System.lineSeparator());
        System.out.print(sb.toString());
    }

    public static void printSolution(Map<Vertex, Integer> distances){
        StringBuilder sb = new StringBuilder("Distances: " + System.lineSeparator());
        for (Map.Entry<Vertex, Integer> entry : distances.entrySet()) {
            sb.append(entry.getKey().id + " -> " +  entry.getValue() + System.lineSeparator());
        }
        System.out.print(sb.toString());
    }
}