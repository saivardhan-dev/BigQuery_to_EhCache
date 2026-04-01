package com.apachecamel.bigquerycacheload.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class MyRecord implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String category;
    private long value;
    private Instant updatedAt;

    public MyRecord() {}

    public MyRecord(String id, String name, String category, long value, Instant updatedAt) {
        this.id        = id;
        this.name      = name;
        this.category  = category;
        this.value     = value;
        this.updatedAt = updatedAt;
    }

    public String getId()          { return id; }
    public String getName()        { return name; }
    public String getCategory()    { return category; }
    public long   getValue()       { return value; }
    public Instant getUpdatedAt()  { return updatedAt; }

    public void setId(String id)                { this.id = id; }
    public void setName(String name)            { this.name = name; }
    public void setCategory(String category)    { this.category = category; }
    public void setValue(long value)            { this.value = value; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MyRecord)) return false;
        MyRecord other = (MyRecord) o;
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "MyRecord{" +
                "id='"       + id       + '\'' +
                ", name='"   + name     + '\'' +
                ", category='" + category + '\'' +
                ", value="   + value    +
                ", updatedAt=" + updatedAt +
                '}';
    }
}