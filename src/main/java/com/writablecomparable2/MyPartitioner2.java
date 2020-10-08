package com.writablecomparable2;

import com.writablecomparable.SortBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner2 extends Partitioner<SortBean, Text> {

    @Override
    public int getPartition(SortBean sortBean, Text text, int numPartitions) {
        switch (text.toString().substring(0, 3)){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
