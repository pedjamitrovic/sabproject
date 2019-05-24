package operations;

import java.sql.Time;
import java.util.Calendar;

public interface GeneralOperations {

    /**
     * Sets initial time
     * @param time time
     */
    void setInitialTime(Calendar time);

    /**
     * Time to pass in simulation.
     * @param days number of days that will pass in simulation after this method call
     * @return current time
     */
    Calendar time(int days);

    /**
     * Gets current time
     * @return current time
     */
    Calendar getCurrentTime();

    /**
     * Clears data in database.
     */
    void eraseAll();
}
