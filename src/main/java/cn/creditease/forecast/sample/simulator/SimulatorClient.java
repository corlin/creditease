package cn.creditease.forecast.sample.simulator;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

public interface SimulatorClient<T> {
	void createTable(T table, byte[][] splitKeys);
	
	//use for emulation
    void put(Put put);
	
	void put(List<Put> puts);
	
	TableName getTableName();
}
