package com.patrick.spark.core;

import com.patrick.spark.advance.SecondarySortKey;
import scala.Serializable;
import scala.math.Ordered;

import java.util.Objects;


public class C7SecondarySortKey implements Ordered<C7SecondarySortKey>, Serializable {

    private int first;
    private int second;

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public C7SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        C7SecondarySortKey that = (C7SecondarySortKey) o;
        return first == that.first &&
                second == that.second;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public int compare( C7SecondarySortKey that) {
        if(this.first - that.getFirst()!=0){
            return this.first-that.getFirst();
        }else{
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less( C7SecondarySortKey that) {
        if(this.first<that.getFirst()){
            return true;
        }else if(this.first==that.getFirst() && this.second<that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater( C7SecondarySortKey that) {
        if(this.first>that.getFirst()){
            return true;
        }else if(this.first==that.getFirst() && this.second>that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq( C7SecondarySortKey that) {
        if(this.$less(that)){
            return true;
        }else if(this.first==that.getFirst() && this.second==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq( C7SecondarySortKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.first==that.getFirst() && this.second==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo( C7SecondarySortKey that) {
        if(this.first - that.getFirst()!=0){
            return this.first-that.getFirst();
        }else{
            return this.second - that.getSecond();
        }
    }
}
