package com.chernykh.db.entity;

import java.util.Objects;

public class Pipeline {

    private int id;
    private int startPointId;
    private int endPointId;
    private int length;

    public Pipeline() {
    }

    public Pipeline(int startPointId, int endPointId, int length) {
        this.startPointId = startPointId;
        this.endPointId = endPointId;
        this.length = length;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getStartPointId() {
        return startPointId;
    }

    public void setStartPointId(int startPointId) {
        this.startPointId = startPointId;
    }

    public int getEndPointId() {
        return endPointId;
    }

    public void setEndPointId(int endPointId) {
        this.endPointId = endPointId;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pipeline)) return false;
        Pipeline pipeline = (Pipeline) o;
        return startPointId == pipeline.startPointId &&
                endPointId == pipeline.endPointId &&
                length == pipeline.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startPointId, endPointId, length);
    }

    @Override
    public String toString() {
        return startPointId +
                ";" + endPointId +
                ";" + length;
    }
}
