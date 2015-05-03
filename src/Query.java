/**
 * Created by andrewgyq on 2015/3/17.
 */
public class Query {
    

    public static void main(String[] args) throws Exception {
       String a = "0.00000468";
       String b = "0.000451";
       System.out.println(Double.valueOf(a) > Double.valueOf(b));
    }
}

/*
actual@pg844.txt    [7/24 , 2/23731 , 0.0000451]
actual@pg84.txt [7/24 , 3/77986 , 0.00002058]
actual@pg5200.txt   [7/24 , 1/25186 , 0.00002125]
actual@pg3825.txt   [7/24 , 1/36585 , 0.00001463]
actual@pg55.txt [7/24 , 1/42603 , 0.00001256]
actual@pg74.txt [7/24 , 4/73837 , 0.00002899]
actual@pg76.txt [7/24 , 1/114266 , 0.00000468]
*/