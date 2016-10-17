package task_1.d;

import common.Util;
import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/*
1.d) Stopwords. Based on a), exclude the stopwords from titles (an exam-
ple list can be obtained at: http://j.mp/STOPWORDS). We refer to
the output of this task as to popular words.

NOTE TO SELF:
"PostTypeId" for Q's = 1.
*/


public class Stopwords {

    private String stopwords = "'ll\n" +
            "a\n" +
            "a's\n" +
            "able\n" +
            "about\n" +
            "above\n" +
            "abst\n" +
            "accordance\n" +
            "according\n" +
            "accordingly\n" +
            "across\n" +
            "act\n" +
            "actually\n" +
            "added\n" +
            "adj\n" +
            "affected\n" +
            "affecting\n" +
            "affects\n" +
            "after\n" +
            "afterwards\n" +
            "again\n" +
            "against\n" +
            "ah\n" +
            "ain't\n" +
            "all\n" +
            "allow\n" +
            "allows\n" +
            "almost\n" +
            "alone\n" +
            "along\n" +
            "already\n" +
            "also\n" +
            "although\n" +
            "always\n" +
            "am\n" +
            "among\n" +
            "amongst\n" +
            "an\n" +
            "and\n" +
            "announce\n" +
            "another\n" +
            "any\n" +
            "anybody\n" +
            "anyhow\n" +
            "anymore\n" +
            "anyone\n" +
            "anything\n" +
            "anyway\n" +
            "anyways\n" +
            "anywhere\n" +
            "apart\n" +
            "apparently\n" +
            "appear\n" +
            "appreciate\n" +
            "appropriate\n" +
            "approximately\n" +
            "are\n" +
            "aren\n" +
            "aren't\n" +
            "arent\n" +
            "arise\n" +
            "around\n" +
            "as\n" +
            "aside\n" +
            "ask\n" +
            "asking\n" +
            "associated\n" +
            "at\n" +
            "auth\n" +
            "available\n" +
            "away\n" +
            "awfully\n" +
            "b\n" +
            "back\n" +
            "be\n" +
            "became\n" +
            "because\n" +
            "become\n" +
            "becomes\n" +
            "becoming\n" +
            "been\n" +
            "before\n" +
            "beforehand\n" +
            "begin\n" +
            "beginning\n" +
            "beginnings\n" +
            "begins\n" +
            "behind\n" +
            "being\n" +
            "believe\n" +
            "below\n" +
            "beside\n" +
            "besides\n" +
            "best\n" +
            "better\n" +
            "between\n" +
            "beyond\n" +
            "biol\n" +
            "both\n" +
            "brief\n" +
            "briefly\n" +
            "but\n" +
            "by\n" +
            "c\n" +
            "c'mon\n" +
            "c's\n" +
            "ca\n" +
            "came\n" +
            "can\n" +
            "can't\n" +
            "cannot\n" +
            "cant\n" +
            "cause\n" +
            "causes\n" +
            "certain\n" +
            "certainly\n" +
            "changes\n" +
            "clearly\n" +
            "co\n" +
            "com\n" +
            "come\n" +
            "comes\n" +
            "concerning\n" +
            "consequently\n" +
            "consider\n" +
            "considering\n" +
            "contain\n" +
            "containing\n" +
            "contains\n" +
            "corresponding\n" +
            "could\n" +
            "couldn't\n" +
            "couldnt\n" +
            "course\n" +
            "currently\n" +
            "d\n" +
            "date\n" +
            "definitely\n" +
            "described\n" +
            "despite\n" +
            "did\n" +
            "didn't\n" +
            "different\n" +
            "do\n" +
            "does\n" +
            "doesn't\n" +
            "doing\n" +
            "don't\n" +
            "done\n" +
            "down\n" +
            "downwards\n" +
            "due\n" +
            "during\n" +
            "e\n" +
            "each\n" +
            "ed\n" +
            "edu\n" +
            "effect\n" +
            "eg\n" +
            "eight\n" +
            "eighty\n" +
            "either\n" +
            "else\n" +
            "elsewhere\n" +
            "end\n" +
            "ending\n" +
            "enough\n" +
            "entirely\n" +
            "especially\n" +
            "et\n" +
            "et-al\n" +
            "etc\n" +
            "even\n" +
            "ever\n" +
            "every\n" +
            "everybody\n" +
            "everyone\n" +
            "everything\n" +
            "everywhere\n" +
            "ex\n" +
            "exactly\n" +
            "example\n" +
            "except\n" +
            "f\n" +
            "far\n" +
            "few\n" +
            "ff\n" +
            "fifth\n" +
            "first\n" +
            "five\n" +
            "fix\n" +
            "followed\n" +
            "following\n" +
            "follows\n" +
            "for\n" +
            "former\n" +
            "formerly\n" +
            "forth\n" +
            "found\n" +
            "four\n" +
            "from\n" +
            "further\n" +
            "furthermore\n" +
            "g\n" +
            "gave\n" +
            "get\n" +
            "gets\n" +
            "getting\n" +
            "give\n" +
            "given\n" +
            "gives\n" +
            "giving\n" +
            "go\n" +
            "goes\n" +
            "going\n" +
            "gone\n" +
            "got\n" +
            "gotten\n" +
            "greetings\n" +
            "h\n" +
            "had\n" +
            "hadn't\n" +
            "happens\n" +
            "hardly\n" +
            "has\n" +
            "hasn't\n" +
            "have\n" +
            "haven't\n" +
            "having\n" +
            "he\n" +
            "he'd\n" +
            "he'll\n" +
            "he's\n" +
            "hed\n" +
            "hello\n" +
            "help\n" +
            "hence\n" +
            "her\n" +
            "here\n" +
            "here's\n" +
            "hereafter\n" +
            "hereby\n" +
            "herein\n" +
            "heres\n" +
            "hereupon\n" +
            "hers\n" +
            "herself\n" +
            "hes\n" +
            "hi\n" +
            "hid\n" +
            "him\n" +
            "himself\n" +
            "his\n" +
            "hither\n" +
            "home\n" +
            "hopefully\n" +
            "how\n" +
            "how's\n" +
            "howbeit\n" +
            "however\n" +
            "hundred\n" +
            "i\n" +
            "i'd\n" +
            "i'll\n" +
            "i'm\n" +
            "i've\n" +
            "id\n" +
            "ie\n" +
            "if\n" +
            "ignored\n" +
            "im\n" +
            "immediate\n" +
            "immediately\n" +
            "importance\n" +
            "important\n" +
            "in\n" +
            "inasmuch\n" +
            "inc\n" +
            "indeed\n" +
            "index\n" +
            "indicate\n" +
            "indicated\n" +
            "indicates\n" +
            "information\n" +
            "inner\n" +
            "insofar\n" +
            "instead\n" +
            "into\n" +
            "invention\n" +
            "inward\n" +
            "is\n" +
            "isn't\n" +
            "it\n" +
            "it'd\n" +
            "it'll\n" +
            "it's\n" +
            "itd\n" +
            "its\n" +
            "itself\n" +
            "j\n" +
            "just\n" +
            "k\n" +
            "keep\n" +
            "keep\n" +
            "keeps\n" +
            "kept\n" +
            "kg\n" +
            "km\n" +
            "know\n" +
            "known\n" +
            "knows\n" +
            "l\n" +
            "largely\n" +
            "last\n" +
            "lately\n" +
            "later\n" +
            "latter\n" +
            "latterly\n" +
            "least\n" +
            "less\n" +
            "lest\n" +
            "let\n" +
            "let's\n" +
            "lets\n" +
            "like\n" +
            "liked\n" +
            "likely\n" +
            "line\n" +
            "little\n" +
            "look\n" +
            "looking\n" +
            "looks\n" +
            "ltd\n" +
            "m\n" +
            "made\n" +
            "mainly\n" +
            "make\n" +
            "makes\n" +
            "many\n" +
            "may\n" +
            "maybe\n" +
            "me\n" +
            "mean\n" +
            "means\n" +
            "meantime\n" +
            "meanwhile\n" +
            "merely\n" +
            "mg\n" +
            "might\n" +
            "million\n" +
            "miss\n" +
            "ml\n" +
            "more\n" +
            "moreover\n" +
            "most\n" +
            "mostly\n" +
            "mr\n" +
            "mrs\n" +
            "much\n" +
            "mug\n" +
            "must\n" +
            "mustn't\n" +
            "my\n" +
            "myself\n" +
            "n\n" +
            "na\n" +
            "name\n" +
            "namely\n" +
            "nay\n" +
            "nd\n" +
            "near\n" +
            "nearly\n" +
            "necessarily\n" +
            "necessary\n" +
            "need\n" +
            "needs\n" +
            "neither\n" +
            "never\n" +
            "nevertheless\n" +
            "new\n" +
            "next\n" +
            "nine\n" +
            "ninety\n" +
            "no\n" +
            "nobody\n" +
            "non\n" +
            "none\n" +
            "nonetheless\n" +
            "noone\n" +
            "nor\n" +
            "normally\n" +
            "nos\n" +
            "not\n" +
            "noted\n" +
            "nothing\n" +
            "novel\n" +
            "now\n" +
            "nowhere\n" +
            "o\n" +
            "obtain\n" +
            "obtained\n" +
            "obviously\n" +
            "of\n" +
            "off\n" +
            "often\n" +
            "oh\n" +
            "ok\n" +
            "okay\n" +
            "old\n" +
            "omitted\n" +
            "on\n" +
            "once\n" +
            "one\n" +
            "ones\n" +
            "only\n" +
            "onto\n" +
            "or\n" +
            "ord\n" +
            "other\n" +
            "others\n" +
            "otherwise\n" +
            "ought\n" +
            "our\n" +
            "ours\n" +
            "ourselves\n" +
            "out\n" +
            "outside\n" +
            "over\n" +
            "overall\n" +
            "owing\n" +
            "own\n" +
            "p\n" +
            "page\n" +
            "pages\n" +
            "part\n" +
            "particular\n" +
            "particularly\n" +
            "past\n" +
            "per\n" +
            "perhaps\n" +
            "placed\n" +
            "please\n" +
            "plus\n" +
            "poorly\n" +
            "possible\n" +
            "possibly\n" +
            "potentially\n" +
            "pp\n" +
            "predominantly\n" +
            "present\n" +
            "presumably\n" +
            "previously\n" +
            "primarily\n" +
            "probably\n" +
            "promptly\n" +
            "proud\n" +
            "provides\n" +
            "put\n" +
            "q\n" +
            "que\n" +
            "quickly\n" +
            "quite\n" +
            "qv\n" +
            "r\n" +
            "ran\n" +
            "rather\n" +
            "rd\n" +
            "re\n" +
            "readily\n" +
            "really\n" +
            "reasonably\n" +
            "recent\n" +
            "recently\n" +
            "ref\n" +
            "refs\n" +
            "regarding\n" +
            "regardless\n" +
            "regards\n" +
            "related\n" +
            "relatively\n" +
            "research\n" +
            "respectively\n" +
            "resulted\n" +
            "resulting\n" +
            "results\n" +
            "right\n" +
            "run\n" +
            "s\n" +
            "said\n" +
            "same\n" +
            "saw\n" +
            "say\n" +
            "saying\n" +
            "says\n" +
            "sec\n" +
            "second\n" +
            "secondly\n" +
            "section\n" +
            "see\n" +
            "seeing\n" +
            "seem\n" +
            "seemed\n" +
            "seeming\n" +
            "seems\n" +
            "seen\n" +
            "self\n" +
            "selves\n" +
            "sensible\n" +
            "sent\n" +
            "serious\n" +
            "seriously\n" +
            "seven\n" +
            "several\n" +
            "shall\n" +
            "shan't\n" +
            "she\n" +
            "she'd\n" +
            "she'll\n" +
            "she's\n" +
            "shed\n" +
            "shes\n" +
            "should\n" +
            "shouldn't\n" +
            "show\n" +
            "showed\n" +
            "shown\n" +
            "showns\n" +
            "shows\n" +
            "significant\n" +
            "significantly\n" +
            "similar\n" +
            "similarly\n" +
            "since\n" +
            "six\n" +
            "slightly\n" +
            "so\n" +
            "some\n" +
            "somebody\n" +
            "somehow\n" +
            "someone\n" +
            "somethan\n" +
            "something\n" +
            "sometime\n" +
            "sometimes\n" +
            "somewhat\n" +
            "somewhere\n" +
            "soon\n" +
            "sorry\n" +
            "specifically\n" +
            "specified\n" +
            "specify\n" +
            "specifying\n" +
            "still\n" +
            "stop\n" +
            "strongly\n" +
            "sub\n" +
            "substantially\n" +
            "successfully\n" +
            "such\n" +
            "sufficiently\n" +
            "suggest\n" +
            "sup\n" +
            "sure\n" +
            "sure\n" +
            "t's\n" +
            "take\n" +
            "taken\n" +
            "tell\n" +
            "tends\n" +
            "th\n" +
            "than\n" +
            "thank\n" +
            "thanks\n" +
            "thanx\n" +
            "that\n" +
            "that's\n" +
            "thats\n" +
            "the\n" +
            "their\n" +
            "theirs\n" +
            "them\n" +
            "themselves\n" +
            "then\n" +
            "thence\n" +
            "there\n" +
            "there's\n" +
            "thereafter\n" +
            "thereby\n" +
            "therefore\n" +
            "therein\n" +
            "theres\n" +
            "thereupon\n" +
            "these\n" +
            "they\n" +
            "they'd\n" +
            "they'll\n" +
            "they're\n" +
            "they've\n" +
            "think\n" +
            "third\n" +
            "this\n" +
            "thorough\n" +
            "thoroughly\n" +
            "those\n" +
            "though\n" +
            "three\n" +
            "through\n" +
            "throughout\n" +
            "thru\n" +
            "thus\n" +
            "to\n" +
            "together\n" +
            "too\n" +
            "took\n" +
            "toward\n" +
            "towards\n" +
            "tried\n" +
            "tries\n" +
            "truly\n" +
            "try\n" +
            "trying\n" +
            "twice\n" +
            "two\n" +
            "un\n" +
            "under\n" +
            "unfortunately\n" +
            "unless\n" +
            "unlikely\n" +
            "until\n" +
            "unto\n" +
            "up\n" +
            "upon\n" +
            "us\n" +
            "use\n" +
            "used\n" +
            "useful\n" +
            "uses\n" +
            "using\n" +
            "usually\n" +
            "value\n" +
            "various\n" +
            "very\n" +
            "via\n" +
            "viz\n" +
            "vs\n" +
            "want\n" +
            "wants\n" +
            "was\n" +
            "wasn't\n" +
            "way\n" +
            "we\n" +
            "we'd\n" +
            "we'll\n" +
            "we're\n" +
            "we've\n" +
            "welcome\n" +
            "well\n" +
            "went\n" +
            "were\n" +
            "weren't\n" +
            "what\n" +
            "what's\n" +
            "whatever\n" +
            "when\n" +
            "when's\n" +
            "whence\n" +
            "whenever\n" +
            "where\n" +
            "where's\n" +
            "whereafter\n" +
            "whereas\n" +
            "whereby\n" +
            "wherein\n" +
            "whereupon\n" +
            "wherever\n" +
            "whether\n" +
            "which\n" +
            "while\n" +
            "whither\n" +
            "who\n" +
            "who's\n" +
            "whoever\n" +
            "whole\n" +
            "whom\n" +
            "whose\n" +
            "why\n" +
            "why's\n" +
            "will\n" +
            "willing\n" +
            "wish\n" +
            "with\n" +
            "within\n" +
            "without\n" +
            "won't\n" +
            "wonder\n" +
            "would\n" +
            "wouldn't\n" +
            "yes\n" +
            "yet\n" +
            "you\n" +
            "you'd\n" +
            "you'll\n" +
            "you're\n" +
            "you've\n" +
            "your\n" +
            "yours\n" +
            "yourself\n" +
            "yourselves\n" +
            "zero";

    public static void main(String[] args) throws Exception {
        // Handle log4j exception errors
        org.apache.log4j.BasicConfigurator.configure();

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Stopwords.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();

            // Get value of "Body" and "PostTypeId"
            String body = Util.getAttrContent("Body", text);
            String postTypeId = Util.getAttrContent("PostTypeId", text);

            // Check PostTypeId and if body is not an empty string
            if(postTypeId.equals("1") && !body.equals("")){
                // Simple/lazy wordsplit
                String[] words = body.split("\\W+");

                // Write words to context
                for (String word : words) {
                    context.write(new Text(word), new IntWritable(1));
                }
            }
        }
    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int totalWords = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            totalWords += sum;
            context.write(key, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text summary = new Text("\nTotal amount of words: ");
            IntWritable sum = new IntWritable(totalWords);
            context.write(summary, sum);
        }
    }

}
