/**
 * Created by jeremyjiang on 2016/6/16.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.customized;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;
import cn.pku.net.db.storm.ndvr.util.SigSim;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: Customized bolt for pre-filtering detection task using textual and global visual signatures
 * this bolt generates and compares textual signatures
 * @author jeremyjiang
 *         Created at 2016/6/16 17:46
 */

public class CusTextGlobalPreDetecBolt extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(CusTextGlobalPreDetecBolt.class);
    private static Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频元数据
    private static Map<String, HSVSigEntity> cachedHSVSignature = new ConcurrentHashMap<String, HSVSigEntity>();    // 缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签

    /**
     * Prepare.
     *
     * @param stormConf the storm conf
     * @param context   the context
     * @see backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext) backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        VideoInfoDao videoInfoDao  = new VideoInfoDao();
        List<VideoInfoEntity> videoInfoList = videoInfoDao.getVideoInfoByDuration(0);    // 取出时长为0的视频(数据集中有些视频没有duration数据,我们设为0)
        if ((null != videoInfoList) && !videoInfoList.isEmpty()) {
            Set<String> videoIdSet = new HashSet<String>();
            for (VideoInfoEntity videoInfoEnt : videoInfoList) {
                videoIdSet.add(videoInfoEnt.getVideoId());
                VideoHSVSigEntity videoHsvSig = (new HSVSignatureDao()).getVideoHSVSigById(videoInfoEnt.getVideoId());
                // 如果数据库中没有该视频的HSV标签,则继续下一个视频
                if (null == videoHsvSig) {
                    continue;
                }
                this.cachedHSVSignature.put(videoHsvSig.getVideoId(), videoHsvSig.getSig());    // 缓存时长为0的视频的HSV全局标签
            }
            this.cachedVideoIdByDuration.put(0, videoIdSet);                                    // 将时长为0的视频缓存
        }
    }

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo1", "queryVideo2", "textSimilarity", "globalDistance",
                "startTimeStamp", "fieldGroupingId"));
    }

    /**
     * Execute.
     *
     * @param input     the input
     * @param collector the collector
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector) backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String          taskId         = input.getStringByField("taskId");
        String          taskType       = input.getStringByField("taskType");
        String          queryVideoStr1 = input.getStringByField("queryVideo1");
        String          queryVideoStr2 = input.getStringByField("queryVideo2");
        long            startTimeStamp = input.getLongByField("startTimeStamp");
        int            fieldGroupingId = input.getIntegerByField("fieldGroupingId");

        VideoInfoEntity queryVideo1    = (new Gson()).fromJson(queryVideoStr1, VideoInfoEntity.class);
        VideoInfoEntity queryVideo2    = (new Gson()).fromJson(queryVideoStr2, VideoInfoEntity.class);

        // initiate enough small textual similarity and enough large global signature distance
        float           textSimilarity = (float) 0.0;
        float           globalDistance = (float) (Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD * 10.0);

        boolean skipText = false;
        boolean skipGlobal = false;

        // 如果两个视频duration相差太大,则文本相似度设为0,输出tuple
        if (Math.abs(queryVideo1.getDuration() - queryVideo2.getDuration())
                > Const.STORM_CONFIG.VIDEO_DURATION_WINDOW) {
            //collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, globalDistance, startTimeStamp, fieldGroupingId));
            skipText = true;
        }

        if (!skipText) {
            String queryVideoText1 = queryVideo1.getTitle();
            String queryVideoText2 = queryVideo2.getTitle();

            // 如果两个query视频的文本信息为空,则文本相似度设为0,输出tuple
            if ((null == queryVideoText1) || (null == queryVideoText2)) {
                //collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, globalDistance, startTimeStamp, fieldGroupingId));
                skipText = true;
            }

            if (!skipText) {
                List<String> querySplitText1 = MyStringUtils.wordSegment(queryVideoText1);
                List<String> querySplitText2 = MyStringUtils.wordSegment(queryVideoText2);

                // 如果两个query视频分词结果为空,则文本相似度设为0,输出tuple
                if (querySplitText1.isEmpty() || querySplitText2.isEmpty()) {
                    //collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, globalDistance, startTimeStamp, fieldGroupingId));
                    skipText = true;
                }

                if (!skipText) {
                    // 计算query与compare相同的term数量占query总term的比例
                    float query1VS2 = (float) 0.0;    // query1与query2逐词比较的相似度
                    int sameTermNum = 0;
                    for (int i = 0; i < querySplitText1.size(); i++) {
                        int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                                ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                        int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText2.size()
                                ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (querySplitText2.size() - 1);

                        for (int j = minIndex; j < maxIndex + 1; j++) {
                            if (querySplitText1.get(i).equals(querySplitText2.get(j))) {
                                sameTermNum++;

                                break;
                            }
                        }
                    }
                    query1VS2 = sameTermNum / (float) querySplitText1.size();

                    // 计算compare与query相同的term数量占compare总term的比例
                    float query2VS1 = (float) 0.0;    // query2与query1逐词比较的相似度
                    sameTermNum = 0;
                    for (int i = 0; i < querySplitText2.size(); i++) {
                        int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                                ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                : 0;
                        int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText1.size()
                                ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                : (querySplitText1.size() - 1);

                        for (int j = minIndex; j < maxIndex + 1; j++) {
                            if (querySplitText2.get(i).equals(querySplitText1.get(j))) {
                                sameTermNum++;

                                break;
                            }
                        }
                    }
                    query2VS1 = sameTermNum / (float) querySplitText2.size();

                    if ((query1VS2 != 0) && (query2VS1 != 0)) {
                        textSimilarity = query1VS2 * query2VS1 / (query1VS2 + query2VS1);
                    }
                }
            }
        }

        if (textSimilarity < Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
            skipGlobal = true;
        }

        HSVSigEntity hsvSignature1 = null;
        HSVSigEntity hsvSignature2 = null;
        if (!cachedHSVSignature.containsKey(queryVideo1.getVideoId())) {
            hsvSignature1 = (new HSVSignatureDao()).getVideoHSVSigById(queryVideo1.getVideoId()).getSig();
            if (null == hsvSignature1) {
                skipGlobal = true;
                logger.error("During comparing, no signature found in database, videoId: " + queryVideo1.getVideoId());
            } else {
                cachedHSVSignature.put(queryVideo1.getVideoId(), hsvSignature1);
            }
        } else {
            hsvSignature1 = cachedHSVSignature.get(queryVideo1.getVideoId());
        }
        if (!skipGlobal && !cachedHSVSignature.containsKey(queryVideo2.getVideoId())) {
            hsvSignature2 = (new HSVSignatureDao()).getVideoHSVSigById(queryVideo2.getVideoId()).getSig();
            if (null == hsvSignature2) {
                skipGlobal = true;
                logger.error("During comparing, no signature found in database, videoId: " + queryVideo2.getVideoId());
            } else {
                cachedHSVSignature.put(queryVideo1.getVideoId(), hsvSignature1);
            }
            cachedHSVSignature.put(queryVideo2.getVideoId(), hsvSignature2);
        } else {
            hsvSignature2 = cachedHSVSignature.get(queryVideo2.getVideoId());
        }

        if (!skipGlobal && null != hsvSignature1 && null != hsvSignature2) {
            // calculate Euclidean global distance
            globalDistance = SigSim.getEuclideanDistance(hsvSignature1, hsvSignature1);
        }
        // 放入文本相似度,输出
        collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, globalDistance, startTimeStamp, fieldGroupingId));
    }
}