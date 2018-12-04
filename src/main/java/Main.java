import org.apache.hadoop.hbase.util.Bytes;

public class Main {

    public static void main(String[] args) {

        HBaseEst hbe = new HBaseEst(args[0]);
        Pair<Long,Long> pair = hbe.getEstimatedRowStats(Bytes.toBytes(args[1]), Bytes.toBytes(args[2]));
        System.out.println("Estimated row count: " + pair.first);
        System.out.println("Estimated byte per rows: " + pair.second);


    }
}
