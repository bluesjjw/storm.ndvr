/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.List;

/**
 * Description: Query task with taskid and videoid list
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:44
 */
public class QueryVideoEntity {
    private String       taskId;
    private List<String> videoIdList;

    /**
     * Getter method for property <tt>taskId</tt>.
     *
     * @return property value of taskId
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * Setter method for property <tt>taskId</tt>.
     *
     * @param taskId value to be assigned to property taskId
     */
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * Getter method for property <tt>videoIdList</tt>.
     *
     * @return property value of videoIdList
     */
    public List<String> getVideoIdList() {
        return videoIdList;
    }

    /**
     * Setter method for property <tt>videoIdList</tt>.
     *
     * @param videoIdList value to be assigned to property videoIdList
     */
    public void setVideoIdList(List<String> videoIdList) {
        this.videoIdList = videoIdList;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
