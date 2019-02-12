package main.java.com.odyssean.util;

public class PNLUtils {

    public static boolean isNull(String str) {
        return str == null ? true : false;
    }

    public static boolean isNullOrBlank(String param) {
        if (isNull(param) || param.equalsIgnoreCase("")) {
            return true;
        }
        return false;
    }

    public static boolean isNumeric(String str)
    {

        return str.chars().allMatch(Character::isDigit);
    }

    public  static boolean isNullSafeNumeric(String str){
        return !isNullOrBlank(str) && isNumeric(str);
    }


}
