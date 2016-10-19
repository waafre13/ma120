package common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Fredric on 17/10/2016.
 */
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

        String content;
        String regex = "(?:"+attrName+"=\")([^\"]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher match = pattern.matcher(text);
        content = match.find() ? match.group(1) : "";

        // Clean the string before return
        return removeHTMLTags(content);
    }

    public static String removeHTMLTags(String string){
        return string.replaceAll("(?i)&(?:[a-z\\d]+|#\\d+|#x[a-f\\d]+);"," ");
    }
}

