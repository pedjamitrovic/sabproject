package student.dijkstra;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Graph {
    public final List<Vertex> vertices;
    public final List<Edge> edges;
    private boolean neighboursInitialized;

    public Graph() {
        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>();
        this.neighboursInitialized = false;
    }

    public void createVertex(int id) {
        Vertex vertex = new Vertex(id);
        vertices.add(vertex);
    }

    public void createEdge(int source, int destination, int weight) {
        Edge edge = new Edge(vertices.get(source), vertices.get(destination), weight);
        edges.add(edge);
        edge = new Edge(vertices.get(destination), vertices.get(source), weight);
        edges.add(edge);
    }

    public Pair<LinkedList<Vertex>, Integer> findShortestPath(int source, int destination){
        initNeighbours();
        Dijkstra dijkstra = new Dijkstra(this);
        return dijkstra.getPath(vertices.get(source), vertices.get(destination));
    }

    public Map<Vertex, Integer> getShortestDistances(int source) {
        initNeighbours();
        Dijkstra dijkstra = new Dijkstra(this);
        return dijkstra.getDistances(vertices.get(source));
    }

    private void initNeighbours(){
        if(neighboursInitialized) return;
        for (Edge edge: edges) edge.initNeighbours();
        neighboursInitialized = true;
    }

}
