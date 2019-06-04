package student.dijkstra;

public class Edge  {
    public final Vertex source;
    public final Vertex destination;
    public final int weight;

    public Edge(Vertex source, Vertex destination, int weight) {
        this.source = source;
        this.destination = destination;
        this.weight = weight;
    }

    public void initNeighbours(){
        source.neighbours.add(destination);
    }
}