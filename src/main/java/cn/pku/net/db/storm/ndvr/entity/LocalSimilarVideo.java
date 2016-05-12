/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.Comparator;

/**
 * Description: Video with similar local signature (e.g. SIFT)
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:42
 */
public class LocalSimilarVideo implements Comparator<LocalSimilarVideo> {
    private String videoId;
    private float  localSigSimilarity;    // 相似的帧占总帧数的比例

    /**
     * Instantiates
     */
    public LocalSimilarVideo() {}

    /**
     * Instantiates with fields
     *
     * @param videoId            the video id
     * @param localSigSimilarity the local sig similarity
     */
    public LocalSimilarVideo(String videoId, float localSigSimilarity) {
        super();
        this.videoId            = videoId;
        this.localSigSimilarity = localSigSimilarity;
    }

    /**
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(LocalSimilarVideo arg0, LocalSimilarVideo arg1) {

        // 相似度较大的在前
        if (arg0.getLocalSigSimilarity() > arg1.getLocalSigSimilarity()) {
            return -1;
        } else if (arg0.getLocalSigSimilarity() < arg1.getLocalSigSimilarity()) {
            return 1;
        }

        return 0;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {}

    /**
     * Getter method for property <tt>localSigSimilarity</tt>.
     *
     * @return property value of localSigSimilarity
     */
    public float getLocalSigSimilarity() {
        return localSigSimilarity;
    }

    /**
     * Setter method for property <tt>localSigSimilarity</tt>.
     *
     * @param localSigSimilarity value to be assigned to property localSigSimilarity
     */
    public void setLocalSigSimilarity(float localSigSimilarity) {
        this.localSigSimilarity = localSigSimilarity;
    }

    /**
     * Getter method for property <tt>videoId</tt>.
     *
     * @return property value of videoId
     */
    public String getVideoId() {
        return videoId;
    }

    /**
     * Setter method for property <tt>videoId</tt>.
     *
     * @param videoId value to be assigned to property videoId
     */
    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
