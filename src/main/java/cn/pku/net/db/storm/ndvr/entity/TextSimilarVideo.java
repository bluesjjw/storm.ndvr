/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.Comparator;

/**
 * Description: Textual similar video with videoid and similarity
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:47
 */
public class TextSimilarVideo implements Comparator<TextSimilarVideo> {
    private String videoId;
    private float  textSimilarity;

    /**
     * Instantiates
     */
    public TextSimilarVideo() {}

    /**
     * Instantiates with fields
     *
     * @param videoId        the video id
     * @param textSimilarity the text similarity
     */
    public TextSimilarVideo(String videoId, float textSimilarity) {
        super();
        this.videoId        = videoId;
        this.textSimilarity = textSimilarity;
    }

    /**
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(TextSimilarVideo arg0, TextSimilarVideo arg1) {
        if (arg0.getTextSimilarity() > arg1.getTextSimilarity()) {
            return -1;
        } else if (arg0.getTextSimilarity() < arg1.getTextSimilarity()) {
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
     * Getter method for property <tt>textSimilarity</tt>.
     *
     * @return property value of textSimilarity
     */
    public float getTextSimilarity() {
        return textSimilarity;
    }

    /**
     * Setter method for property <tt>textSimilarity</tt>.
     *
     * @param textSimilarity value to be assigned to property textSimilarity
     */
    public void setTextSimilarity(float textSimilarity) {
        this.textSimilarity = textSimilarity;
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
