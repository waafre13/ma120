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
        return getAttrContent(attrName, text, false);
    }

    public static String getAttrContent(String attrName, String text, Boolean removeTagContent){

        String regex = "(?:"+attrName+"=\")([^\"]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher match = pattern.matcher(text);
        if(match.find()){
            String string = match.group(1);
            if(removeTagContent){
                string.replaceAll("(?:&lt;)([^&gt;])*", "");
            }
            return removeHTMLTags(string);                // Content of HTML tags) : "";
        } else{
            return "";
        }
    }

    public static String removeHTMLTags(String string){
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

    /*
    public class KeyValueArray{

        private int[] keyArray = new int[];
        private String[] valArray = new String[];

        public KeyValueArray()
        {
        }

        public KeyValueArray(int[] key, String[] val){
            keyArray = key;
            valArray = val;
        }

        public int[] getKeyArray(){
            return keyArray;
        }

        public String[] getValArray(){
            return valArray;
        }

        public void sortArraysByKey(){

            // Sort by reputation, descending
            for (int i = 0; i < keyArray.length; i++){
                for (int j = 0; j < keyArray.length; j++){
                    if(keyArray[i] > keyArray[j]){

                        int tempKey = keyArray[j];
                        keyArray[j] = keyArray[i];
                        keyArray[i] = tempKey;

                        String tempVal = valArray[j];
                        valArray[j] = valArray[i];
                        valArray[i] = tempVal;
                    }
                }
            }
        }

        public void sortArraysByValue(){

            // Sort by reputation, descending
            for (int i = 0; i < valArray.length; i++){
                for (int j = 0; j < valArray.length; j++){
                    if(valArray[i] > valArray[j]){

                        int tempKey = valArray[j];
                        valArray[j] = valArray[i];
                        valArray[i] = tempKey;

                        String tempVal = keyArray[j];
                        keyArray[j] = keyArray[i];
                        keyArray[i] = tempVal;
                    }
                }
            }

        }
    }*/
}

