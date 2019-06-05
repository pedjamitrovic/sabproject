package student.dijkstra.test;

import java.util.LinkedList;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import student.dijkstra.*;

public class DijkstraTest {
    @Test
    public void dijkstraTest() {
        Graph graph = new Graph();

        for (int id = 0; id < 7; id++) graph.createVertex(id);

        graph.createEdge(0, 1, 8);
        graph.createEdge(0, 2, 2);
        graph.createEdge(1, 3, 10);
        graph.createEdge(2, 3,15);
        graph.createEdge(3, 4, 3);
        graph.createEdge(3, 5, 3);
        graph.createEdge(4, 6, 2);
        graph.createEdge(5, 6, 1);

        Pair<LinkedList<Vertex>, Integer> path = graph.findShortestPath(0,0);

        Assert.assertNotNull(path);
        Assert.assertTrue(path.first.size() > 0);

        Dijkstra.printSolution(path); // Path: 0 -> 2 -> 3 -> 5 -> 6 Distance: 21

        Map<Vertex, Integer> distances = graph.getShortestDistances(0);

        Dijkstra.printSolution(distances);

    }
}