package com.fh.daml.datawarehouse.operation.utils;

import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * Author:zwj
 * Date:2020/1/14 15:49
 * Description:
 * Modified By:
 */
public class CalculateUtils {


    /**
     * @Author ZZW
     * @Param [d1, d2]
     * @Return java.lang.String
     * @Date 2019/10/22 18:22
     * @Description 计算百分比
     **/

    public static String getPercent(String d1, String d2) {
        if (StringUtils.isEmpty(d1) || StringUtils.isEmpty(d2) || "0".equals(d2) || "0".equals(d1)) {
            return "0%";
        }
        BigDecimal a = new BigDecimal(d1);
        BigDecimal b = new BigDecimal(d2);
        DecimalFormat df = new DecimalFormat("0.0000%");
        String result = df.format(a.divide(b, 6, RoundingMode.HALF_UP));
        result = result.equals("0.0000%") ? "0%" : result;
        result = result.equals("100.0000%") ? "100%" : result;
        return result;
    }


    /**
     * @Author ZZW
     * @Param [d1, d2]
     * @Return java.lang.String
     * @Date 2019/10/22 18:55
     * @Description 计算百分比
     **/
    public static String getPerCents(String d1, String d2) {
        if (StringUtils.isEmpty(d1) || StringUtils.isEmpty(d2) || "0".equals(d2) || "0".equals(d1)) {
            return "0%";
        }
        BigDecimal a = new BigDecimal(d1);
        BigDecimal b = new BigDecimal(d2);
        DecimalFormat df = new DecimalFormat("0%");
        return df.format(a.divide(b, 2, RoundingMode.HALF_UP));
    }
}
