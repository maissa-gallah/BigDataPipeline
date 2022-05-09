package com.bigdata.spark.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.bigdata.spark.entity.Temperature;

public class Mapper {

    public static Temperature parseToIotData(Object[] columns) {
        Date timestamp1 = null;
        try {
            timestamp1 = new SimpleDateFormat("yyyy-MM-dd").parse(columns[0].toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Temperature ioTData = new Temperature(
                columns[1].toString(),
                Double.valueOf(columns[2].toString()),
                new java.sql.Date(timestamp1.getTime())
        );
        return ioTData;
    }
}
