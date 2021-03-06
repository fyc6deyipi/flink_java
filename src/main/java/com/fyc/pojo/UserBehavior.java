package com.fyc.pojo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class UserBehavior {
    public String user_id;
    public String item_id;
    public String category_id;
    public String behavior;
    public String ts;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long timestamp =0;

    public UserBehavior(String user_id, String item_id, String category_id, String behavior, String ts) {
        this.user_id = user_id;
        this.item_id = item_id;
        this.category_id = category_id;
        this.behavior = behavior;
        this.ts = ts;
        try {
            this.timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(this.ts).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    public UserBehavior(){}

    public String json;
    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getItem_id() {
        return item_id;
    }

    public void setItem_id(String item_id) {
        this.item_id = item_id;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
    public UserBehavior setByJson(String json){
        JSONObject jsonObject = JSON.parseObject(json);
        this.user_id=jsonObject.getString("user_id");
        this.item_id=jsonObject.getString("item_id");
        this.category_id=jsonObject.getString("category_id");
        this.behavior=jsonObject.getString("behavior");
        this.ts=jsonObject.getString("ts");
        this.json=json;
        try {
            this.timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(this.ts).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return this;
    }
    @Override
    public String toString() {
        return "UserBehavior{" +
                "user_id='" + user_id + '\'' +
                ", item_id='" + item_id + '\'' +
                ", category_id='" + category_id + '\'' +
                ", behavior='" + behavior + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return (this.user_id+this.ts).hashCode();
    }
}
