package com.iris.egrant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class KeyCotfVO implements Writable, DBWritable { 

	/**
	 * 关键词组合
	 */
	private String  tmp_key_cotf ;
	/**
	 * 关键词组合数量
	 */
	private int  key_combine_size ;
	/**
	 * cotf值
	 */
	private int  cotf ;
	
	
	 
	public KeyCotfVO() {
		super();
	}
	public String getTmp_key_cotf() {
		return tmp_key_cotf;
	}
	public void setTmp_key_cotf(String tmp_key_cotf) {
		this.tmp_key_cotf = tmp_key_cotf;
	}
	public int getKey_combine_size() {
		return key_combine_size;
	}
	public void setKey_combine_size(int key_combine_size) {
		this.key_combine_size = key_combine_size;
	}
	public int getCotf() {
		return cotf;
	}
	public void setCotf(int cotf) {
		this.cotf = cotf;
	}
	
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, tmp_key_cotf);
        statement.setInt(2, key_combine_size);
        statement.setInt(3, cotf);
	}
	
	public void readFields(ResultSet resultSet) throws SQLException {
		tmp_key_cotf = resultSet.getString(1) ;
		key_combine_size =resultSet.getInt(2);
		cotf =resultSet.getInt(3);
		
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(tmp_key_cotf);
		out.writeInt(key_combine_size);
		out.writeInt(cotf);
	}
	
	public void readFields(DataInput in) throws IOException {
		tmp_key_cotf = in.readUTF();
		key_combine_size = in.readInt();
		cotf = in.readInt();
	}

	@Override
	public String toString() {
		return  tmp_key_cotf  +"@##@" + key_combine_size+"@##@" + cotf ;
	}
}
