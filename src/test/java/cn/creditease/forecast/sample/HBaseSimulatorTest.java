package cn.creditease.forecast.sample;

/**
 * Created by corlinchen on 2017/4/20.
 */
import cn.creditease.forecast.sample.BeanFactory;
import cn.creditease.forecast.sample.HashChoreWoker;
import cn.creditease.forecast.sample.PartitionRowKeyManager;
import cn.creditease.forecast.sample.simulator.simple.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TestHBaseAdminNoCluster;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.apache.hadoop.hbase.util.Bytes;

import  cn.creditease.forecast.sample.simulator.HBaseSimulator;

public class HBaseSimulatorTest {


    private  HBaseSimulator hbase = BeanFactory.getInstance().getBeanInstance(HBaseSimulator.class);
    private RowKeyGenerator rkGen = BeanFactory.getInstance().getBeanInstance(RowKeyGenerator.class);

    HashChoreWoker worker = new HashChoreWoker(100000,10);


    @Test
    public void testHash(){
        byte [][] splitKeys = worker.calcSplitKeys();
        hbase.createTable("user", splitKeys);
        TableName tableName = TableName.valueOf("user");
        for(int i = 0; i < 100000; i ++) {
            //System.out.println(new String(rkGen.nextId()));
            Put put = new Put(rkGen.nextId());
            hbase.put(tableName, put);
        }

        hbase.report(tableName);
    }


    @Test
    public void testPartition(){
        //default 20 partitions.
        PartitionRowKeyManager rkManager = new PartitionRowKeyManager();

        byte [][] splitKeys = rkManager.calcSplitKeys();
        //System.out.println(splitKeys);
        hbase.createTable("person", splitKeys);

        TableName tableName = TableName.valueOf("person");

        for(int i = 0; i < 10; i ++) {
            //System.out.println(new String(rkManager.nextId()));
            Put put = new Put(rkManager.nextId());
            hbase.put(tableName, put);
        }

        hbase.report(tableName);
    }

}