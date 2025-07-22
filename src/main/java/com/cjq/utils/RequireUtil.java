package com.cjq.utils;

import com.cjq.exception.ErrorCode;
import com.cjq.exception.EsSqlParseException;

public class RequireUtil {
    public static <T> T requireCastType(Object obj, Class<T> clazz) {
        if (clazz.isInstance(obj)) {
            return clazz.cast(obj);
        } else {
            throw new EsSqlParseException(ErrorCode.TYPE_CASE, obj.getClass() + " do not cast to " + clazz.getName() + "!");
        }
    }

    public static <T> void requireTypeCheck(Object obj, Class<T> clazz) {
        if (!clazz.isInstance(obj)) {
            throw new EsSqlParseException(ErrorCode.TYPE_CASE, obj.getClass() + " is not instance of " + clazz.getName() + "!");
        }
    }

    public static void requireNotNull(Object obj) {
        if (obj == null) {
            throw new EsSqlParseException("require not null!");
        }
    }
}
