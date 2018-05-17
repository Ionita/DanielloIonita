package main.java.entities;

import java.io.Serializable;
import java.util.Date;

public class SorterClass implements Serializable{
    private int id;
    private int timestamp;
    private double value;
    private int property;
    private int plugid;
    private int householdid;
    private int houseid;
    private int timezone; // 0: 00.00->05.59, 1: 06.00->11.59, 2: 12.00->17.59, 3: 18.00->23.59

    public SorterClass(int id, int timestamp, double value, int property, int plugid, int householdid, int houseid) {
        this.id = id;
        this.timestamp = timestamp;
        this.value = value;
        this.property = property;
        this.plugid = plugid;
        this.householdid = householdid;
        this.houseid = houseid;
        int moduloDay = (timestamp%86400);
        if (moduloDay < 21600)
            timezone = 0;
        else if(moduloDay < 43200)
            timezone = 1;
        else if(moduloDay < 64800)
            timezone = 2;
        else
            timezone = 3;

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int isProperty() {
        return property;
    }

    public void setProperty(int property) {
        this.property = property;
    }

    public int getPlugid() {
        return plugid;
    }

    public void setPlugid(int plugid) {
        this.plugid = plugid;
    }

    public int getHouseholdid() {
        return householdid;
    }

    public void setHouseholdid(int householdid) {
        this.householdid = householdid;
    }

    public int getHouseid() {
        return houseid;
    }

    public void setHouseid(int houseid) {
        this.houseid = houseid;
    }

    public int getTimezone() {
        return timezone;
    }
}
