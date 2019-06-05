package student;

public class Line{
    public int id;
    public int firstCity;
    public int secondCity;
    public int distance;
    public Line(int id, int firstCity, int secondCity, int distance){
        this.id = id;
        this.firstCity = firstCity;
        this.secondCity = secondCity;
        this.distance = distance;
    }
    public String getHashKey(){
        return firstCity < secondCity ? firstCity + "#" + secondCity : secondCity + "#" + firstCity;
    }
    public static String getHashKey(int firstCity, int secondCity){
        return firstCity < secondCity ? firstCity + "#" + secondCity : secondCity + "#" + firstCity;
    }
}