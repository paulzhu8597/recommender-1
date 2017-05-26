package com.haozhuo.bigdata.rec.bean;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;

import java.io.Serializable;

public class Users implements Serializable {
    // Column 0
    public final int id;
    // Column 1
    public final String name;
    // Column 2
    public RecordIdentifier rowId;

    public Users(int id, String name, RecordIdentifier rowId) {
        this.id = id;
        this.name = name;
        this.rowId = rowId;
    }

    public Users(int id, String name) {
        this.id = id;
        this.name = name;
        rowId = null;
    }

    @Override
    public String toString() {
        return "Users [id=" + id + ", name=" + name + ", rowId=" + rowId + "]";
    }
}
