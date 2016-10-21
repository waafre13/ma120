package common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    // Returns attribute content for multiple attributes in an array
    public static String[] getAttrContent(String[] attrNames, String text){

        String[] contentList = new String[attrNames.length];
        int i = 0;
        for (String attrName: attrNames) {
            contentList[i] = getAttrContent(attrName, text);
            i++;
        }

        return contentList;
    }

    // Returns attribute content for a single attribute
    public static String getAttrContent(String attrName, String text){
        return getAttrContent(attrName, text, false);
    }

    public static String getAttrContent(String attrName, String text, Boolean removeTagContent){

        String regex = "(?:"+attrName+"=\")([^\"]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher match = pattern.matcher(text);
        if(match.find()){
            String string = match.group(1);
            if(removeTagContent){
                string.replaceAll("(?:&lt;)([^&gt;])*", "");            // Content of HTML tags) : "";
            }
            return cleanString(string);
        } else{
            return "";
        }
    }

    public static String cleanString(String string){
        return string
                .replaceAll("(?=[^\\d])?(\\.)(?!\\d)", " ")             // Periods(not surrounded by digits) -> whitespace
                .replaceAll("(&#xA;)", " ")                             // Newline -> whitespace
                .replaceAll("(?i)&([a-z\\d]+|#\\d+|#x[a-f\\d]+);","")   // HTML tags
                .replaceAll("((https*://|w{3}\\.)[^\\s]+)","")          // Links
                .replaceAll("(\\w:/*\\*[^\\s]+)","")                    // Filepaths
                .replaceAll("(\\s')|'(?!\\w)","")                       // Apostrophs that are _not_ inside a word
                .replaceAll("([^\\w\\-.'\\s])","");                     // Symbols that are not - . ' whitespace or words
    }

    public static boolean isInteger(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}

