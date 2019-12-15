package com.patrick.spark.advance;

import scala.math.Ordered;

import java.io.Serializable;


/**
 * 自定义的二次排序key
 * 即如果给定的第一个排序的key是重复的 用第二个排序的key排序
 */
public class SecondarySortKey  implements Ordered<SecondarySortKey>,Serializable {

    private static final long serialVersionUID = -8971131036393074449L;
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }


    @Override
    public int compare( SecondarySortKey that) {
        if(this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        }

        return this.second - that.getSecond();
    }

    @Override
    public boolean $less( SecondarySortKey that) {

        if(this.first < that.getFirst()){
            return true;
        }else if (this.first == that.getFirst() && this.second < that.getSecond()){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $greater( SecondarySortKey that) {
        if(this.first > that.getFirst()){
            return true;
        }else if (this.first == that.getFirst() && this.second> that.getSecond()){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $less$eq( SecondarySortKey that) {
        if(this.$less(that)){
            return true;
        }else if(this.first == that.getFirst() && this.second==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq( SecondarySortKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.first == that.getFirst() && this.second==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo( SecondarySortKey that) {
        if(this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        }

        return this.second - that.getSecond();
    }

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
}
