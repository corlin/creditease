package cn.creditease.forecast.sample.simulator;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

public interface HBaseSimulator<T>   {
	
	void report(TableName tableName);
	
	//use for emulation
    void put(TableName tableName, Put put);
	
	void put(TableName tableName, List<Put> puts);
	
	void createTable(T table, byte[][] splitKeys);
}
