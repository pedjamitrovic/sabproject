package student;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Settings {
    public static String connectionUrl = "jdbc:sqlserver://localhost:1433;databaseName=DomaciSAB;user=pedja;password=";
    public static BigDecimal fixBigDecimal(BigDecimal d){
        BigDecimal temp = d.stripTrailingZeros();
        return new BigDecimal(temp.toPlainString());
    }
}
