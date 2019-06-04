package student.dijkstra;

import java.util.ArrayList;
import java.util.List;

public class Vertex {
    public final int id;
    public final List<Vertex> neighbours;

    public Vertex(int id) { this.id = id; this.neighbours = new ArrayList<>(); }
    @Override public String toString() { return Integer.toString(id); }
}