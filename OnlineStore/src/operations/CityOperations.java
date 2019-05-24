package operations;

import java.util.List;

public interface CityOperations {

    /**
     * Creates new city.
     *
     * @param name the name of the city. Name of the cities must be unique.
     * @return city id, or -1 on failure
     */
    int createCity(String name);

    /**
     * Gets all cities
     * @return ids of cities, null if failure
     */
    List<Integer> getCities();

    /**
     * Connects two cities. There can be max one line between cities.
     * @param cityId1 id of the first city
     * @param cityId2 id of the second city
     * @param distance distance between cities (distance is measured in days)
     * @return line id, or -1 on failure
     */
    int connectCities(int cityId1, int cityId2, int distance);

//    /**
//     * Disconnects two cities. Cities that are not connected can not be disconnected.
//     * @param cityId1 - id of the first city
//     * @param cityId2 - id of the second city
//     * @return 1 on success, -1 on failure
//     */
//    int disconnectCities(int cityId1, String cityId2);

    /**
     * Get connected cities.
     * @param cityId id of the city that connections are asked for
     * @return list of connected cities ids
     */
    List<Integer> getConnectedCities(int cityId);

    /**
     * Get shops in the city.
     * @param cityId id of the city
     * @return list of ids of shops, null if failure
     */
    List<Integer> getShops(int cityId);
}
