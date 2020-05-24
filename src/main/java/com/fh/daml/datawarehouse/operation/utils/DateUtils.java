package com.fh.daml.datawarehouse.operation.utils;


import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;

/**
 * Author:zwj
 * Date:2019/11/15 10:15
 * Description:
 * Modified By:
 */
public class DateUtils {

    /**
     * 获取某周的开始日期
     *
     * @param offset 0本周，1下周，-1上周，依次类推
     * @return
     */
    public static LocalDate weekStart(int offset) {
        LocalDate localDate = LocalDate.now().plusWeeks(offset);
        return localDate.with(DayOfWeek.MONDAY);
    }
}
