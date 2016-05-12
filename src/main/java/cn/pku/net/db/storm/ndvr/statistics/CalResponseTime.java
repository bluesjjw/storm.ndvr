/**
 * @Package cn.pku.net.db.storm.ndvr.statistics
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.statistics;

import java.io.FileWriter;
import java.io.IOException;

import java.util.List;

import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;

/**
 * Description: Calculate the response time
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:32
 */
public class CalResponseTime {

    /**
     * Calculate.
     *
     * @throws IOException the io exception
     */
    public void calculate() throws IOException {
        TaskResultDao    dao    = new TaskResultDao();
        List<TaskEntity> result = dao.getTaskResult();

        System.out.println(result.size());

        long   totalTime = 0;
        long   minTime   = 10000000;
        long   maxTime   = 0;
        String fileName  =
            "E:\\云盘\\文档\\pku\\paper\\NDVR\\实验\\ExperimentResult\\general_with_reduction\\global_no_similar_duration.txt";
        FileWriter writer = new FileWriter(fileName, true);

        for (TaskEntity ent : result) {
            writer.write(ent.getTaskId() + ",");

            String videoIdListStr = "";

            for (String id : ent.getVideoIdList()) {
                videoIdListStr += id + ",";
            }

            writer.write(videoIdListStr + "\n");

            long responseTime = Long.parseLong(ent.getTimeStamp());

            if (responseTime < minTime) {
                minTime = responseTime;
            } else if (responseTime > maxTime) {
                maxTime = responseTime;
            }

            totalTime += responseTime;
        }

        writer.close();    // 关闭输出流
        System.out.println("Average response time: " + (double) totalTime / (double) result.size());
        System.out.println("Min response time: " + minTime);
        System.out.println("Max response time: " + maxTime);
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        CalResponseTime c = new CalResponseTime();

        c.calculate();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
