/**
 * @Package cn.pku.net.db.storm.ndvr.common
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Description: Constant configuration
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:05
 */
public class Const {
    public static int THREAD_POOL_MAX_NUM = 10;    // the max number in thread pool

    /**
     * CC_WEB_VIDEO dataset related config
     */
    public static class CC_WEB_VIDEO {

        // PC
        // public static String VIDEO_INFO_PATH            = "E:\\dataset\\cc_web_video\\Video_Complete.txt";
        // public static String KEYFRAME_INFO_PATH         = "E:\\dataset\\cc_web_video\\Shot_Info.txt";
        // public static String HSV_SIGNATURE_PATH  = "E:\\dataset\\cc_web_video\\HSV.txt";
        // public static String KEYFRAME_PATH_PREFIX  = "E:\\dataset\\cc_web_video\\keyframe\\";
        // public static String SIFT_SIGNATURE_PATH_PREFIX = "E:\\dataset\\cc_web_video\\siftsignature\\";
        // Server
        public static String VIDEO_INFO_PATH            = "/home/jiangjw/dataset/cc_web_video/Video_Complete.txt";    // 视频元数据文件
        public static String KEYFRAME_INFO_PATH         = "/home/jiangjw/dataset/cc_web_video/Shot_Info.txt";    // 关键帧元数据文件
        public static String VIDEO_PATH_PREFIX          = "/home/jiangjw/dataset/cc_web_video/video/";    // 视频文件目录
        public static String KEYFRAME_PATH_PREFIX       = "/home/jiangjw/dataset/cc_web_video/keyframe/";    // 关键帧文件目录
        public static String HSV_SIGNATURE_PATH         = "/home/jiangjw/dataset/cc_web_video/HSV.txt";    // HSV标签文件
        public static String SIFT_SIGNATURE_PATH_PREFIX = "/home/jiangjw/dataset/cc_web_video/siftsignature/";    // SIFT标签文件目录
    }


    /**
     * MongoDB related config
     */
    public static class MONGO {
        public static String MONGO_TASK_HOST              = "162.105.146.209";    // MongoDB host sending tasks
        public static String MONGO_HOST                   = "localhost";          // MongoDB host
        public static int    MONGO_PORT                   = 27017;                // MongoDB port
        public static String MONGO_DATABASE               = "NDVR";               // MongoDB database name
        public static String MONGO_TASK_COLLECTION        = "task";               // task collection's name
        public static String MONGO_VIDEO_COLLECTION       = "videoInfo";          // video info collection's name
        public static String MONGO_KEYFRAME_COLLECTION    = "keyFrame";           // keyframe collection's name
        public static String MONGO_HSVSIG_COLLECTION      = "hsvSignature";       // HSV signature collection's name
        public static String MONGO_TASK_RESULT_COLLECTION = "taskResult";         // task result collection's name
    }


    /**
     * Storm related config.
     */
    public static class STORM_CONFIG {
        public static boolean IS_LOCAL_MODE                          = false;          // local mode switch
        public static boolean IS_DEBUG                               = true;           // debug log switch
        public static int     GET_TASK_INTERVAL                      = 100;            // task interval, in milliseconds
        public static String  RETRIEVAL_TASK_FLAG                    = "retrieval";    // the retrieval task flag
        public static String  DETECTION_TASK_FLAG                    = "detection";    // the detection task flag
        public static int     TEXT_COMPARED_WINDOW                   = 3;              // 比较两个视频文本信息时,单词的窗口大小
        public static int     VIDEO_DURATION_WINDOW                  = 20;             // 确定待比较的视频候选集时,视频时长的比较窗口大小
        public static int     BOLT_DURATION_WINDOW                   = 60;             // 每个bolt负责一段时间的视频,单位为秒
        public static int     FRAME_COMPARED_WINDOW                  = 3;              // 比较local标签时,帧间比较窗口的大小
        public static float   GLOBALSIG_EUCLIDEAN_THRESHOLD          = (float) 0.8;    // 全局标签的欧氏距离阈值
        public static float   GLOBALSIG_EUCLIDEAN_TRUST_THRESHOLD    = (float) 0.2;    // 全局标签的欧氏距离的可信阈值,小于此阈值就可以认为是近似重复视频
        public static int     LOCALSIG_KEYFRAME_MAXNUM               = 10;    // 计算SIFT标签时,最多使用前几个帧,避免有的视频帧太多,导致计算时间太长
        public static float   TEXT_SIMILARITY_THRESHOLD              = (float) 0.5;    // 文本信息相似度的阈值
        public static float   LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD = (float) 0.1;    // 两个帧图像之间,相似的SIFT keyponts数量达到一定比例即认为两个帧图像相似
        public static float   LOCALSIG_VIDEO_SIMILARITY_THRESHOLd = (float) 0.5;    // 两个视频之间,相似的帧图像数量达到一定的比例即认为两个视频相似
        public static boolean IS_FILTER_AND_REFINE                = true;           // 在使用多种特征时,是否是filter-and-refine的策略
    }

    /**
     * Shared Stream Message related config.
     */
    public static class SSM_CONFIG {
        public static boolean IS_REDUCTIION = true;    // SSM reduction switch, 是否缩减控制信息

        /**
         * Storm Topology graph
         */
        public static int[][] TOPOLOGY_GRAPH = {
            { 0, 1, 0, 0 }, { 0, 0, 1, 0 }, { 0, 0, 0, 1 }, { 0, 0, 0, 0 }
        };

        /**
         * Component names in the Storm topology
         */
        public static String[] TOPOLOGY_COMPONENT = { "GetTaskSpout", "GlobalFeatureBolt", "GlobalSigDistance",
                                                      "GlobalResultBolt" };
        public static Map<String, String> COMPONENT_KEY_NEEDED = new HashMap<String, String>();    // fields needed by each component
        public static Map<String, String> COMPONENT_NEW_KEY = new HashMap<String, String>();    // fields produced by each component
        public static Map<String, String> COMPONENT_DISCARD_KEY = new HashMap<String, String>();    // fields should discarded by each component

        {
            COMPONENT_KEY_NEEDED.put("GlobalFeatureBolt", "queryVideo");
            COMPONENT_NEW_KEY.put("GlobalFeatureBolt", "keyframeList||globalSignature");
            COMPONENT_KEY_NEEDED.put("GlobalSigDistanceBolt", "queryVideo||globalSignature");
            COMPONENT_NEW_KEY.put("GlobalSigDistanceBolt", "globalSimilarVideoList");
            COMPONENT_KEY_NEEDED.put("LocalFeatureBolt", "queryVideo||keyframeList");
            COMPONENT_NEW_KEY.put("LocalFeatureBolt", "localSignature");
            COMPONENT_KEY_NEEDED.put("LocalSigDistanceBolt", "queryVideo||localSignature");
            COMPONENT_NEW_KEY.put("LocalSigDistanceBolt", "localSimilarVideoList");
            COMPONENT_KEY_NEEDED.put("TextSimilarityBolt", "queryVideo");
            COMPONENT_NEW_KEY.put("TextSimilarityBolt", "textSimilarVideoList");

            // global
            COMPONENT_DISCARD_KEY.put("GlobalSigDistanceBolt", "queryVideo,keyframeList,globalSignature");

            // global+local
            // COMPONENT_DISCARD_KEY.put("LocalSigDistanceBolt",
            // "queryVideo,globalSimilarVideoList,localSignature");
            // COMPONENT_DISCARD_KEY.put("LocalFeatureBolt", "keyframeList");
            // COMPONENT_DISCARD_KEY.put("GlobalSigDistanceBolt", "globalSignature");
            // text+global
            // COMPONENT_DISCARD_KEY.put("GlobalSigDistanceBolt",
            // "queryVideo,keyframeList,globalSignature,textSimilarVideoList");
            // text
            // COMPONENT_DISCARD_KEY.put("TextSimilarityBolt", "queryVideo");
        }

        /**
         * Discard invalid key map.
         *
         * @param boltId  the bolt id
         * @param ctrlMsg the ctrl msg
         * @return the map
         */
        public static Map<String, String> discardInvalidKey(String boltId, Map<String, String> ctrlMsg) {
            if (!COMPONENT_DISCARD_KEY.containsKey(boltId)) {
                return ctrlMsg;
            }

            String[] discaredKeys = COMPONENT_DISCARD_KEY.get(boltId).split(",");

            for (String key : discaredKeys) {
                ctrlMsg.remove(key);
            }

            return ctrlMsg;
        }
    }

}


//~ Formatted by Jindent --- http://www.jindent.com
