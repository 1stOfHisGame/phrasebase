import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.*;
import java.util.*;
import java.io.*;
import java.io.FileWriter;  

//for tagme
import java.net.*;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
import java.util.Map;
import java.util.Iterator;
//import org.json.simple.parser.*;
import java.util.LinkedHashMap;
import org.json.JSONArray;
import org.json.JSONObject;

//hadoop packages
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//stanford packages
import edu.stanford.nlp.process.*;
import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;
import edu.stanford.nlp.trees.tregex.*;

public class PhraseBase{
static String VP="";
static String word="";
static String sub_tag="";
static String ob_tag="";
static String sub_res="";
static String ob_res="";
static int flag0=0;
static int flag1=0;
static boolean phrase_unknown=true;
static boolean need_chain=true;
static ArrayList<String> sub_list = new ArrayList<String>();
static ArrayList<String> ob_list = new ArrayList<String>();
static boolean sub_change=true;
static boolean ob_change=true;
static ArrayList<Tuple> tup = new ArrayList<Tuple>();
static FileWriter fw;
static String clean(String z){
	z=z.replace("<","");
	z=z.replace(">","");
	return z;
}
public static class TagmeMapper
       extends Mapper<Object, Text, Text, Text>{
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException{
                   // System.out.println("qwertyuiopasdfghjklzxcvbnm");
                    if(flag0 == 1 && flag1 == 1)
                    	{
                    		//System.out.println(sub_res);System.out.println(ob_res);
                    		//context.write(new Text("subject ontology: "+sub_res+"\n"),new Text("object ontology: "+ob_res+"\n"));
                    		return;
                    	
                    	}
                    
                    if(sub_tag.isEmpty() == false || ob_tag.isEmpty() == false){
                    Pattern p = Pattern.compile("<(.*?)>");
		      Matcher m = p.matcher(value.toString());
		    int i = 0;
		    String[] token = new String[5];
		    while(m.find())
		    {
		    token[i++] = m.group(0);
		       	/*
		    	0=res
			1=onto
			*/
		    }
		    
                    p = Pattern.compile("\"(.*?)\"");
		    m = p.matcher(value.toString());
		    String token1 = new String();
		    while(m.find())
		    {
		    token1 = m.group(0);
		       	/*
		    	2=id
			*/
		    }
		    if(token1.isEmpty()==false){
		    //System.out.println("res: "+token[0]+" onto: "+token[1]+" id: "+token1);
		  if(sub_tag.isEmpty() == false)
		    if((token1.substring(1,token1.length()-1)).equals(sub_tag))
		    	{
		    		sub_res = token[1];
		    		context.write(new Text("subject ontology: "),new Text(sub_res));
		    		flag0=1;
		    		fw.write("<div><b>Subject Ontology</b><br>"+clean(sub_res)+"<br>");
		    }
		  if(ob_tag.isEmpty() == false) 
		    if((token1.substring(1,token1.length()-1)).equals(ob_tag))
		    	{
		    		ob_res = token[1];
		    		context.write(new Text("object ontology: "),new Text(ob_res));
		    		flag1=1;
		    		fw.write("<b>Object Ontology</b><br>"+clean(ob_res)+"<br></div>");	
		    }
		    
                   }
                    //else {flag0=1;flag1=1;}
                   }
           	
       }
      }

public static class TagmeReducer
       extends Reducer<Text,Text,Text,Text>{
       	
       	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException{
      for(Text val:values)
 	     context.write(key,val);
                       	
                 }
            }



public static class PBMapperInter
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                    	
      Pattern p = Pattern.compile("<(.*?)>");
      Matcher m = p.matcher(value.toString());
    int i = 0;
    String[] token = new String[7];
    while(m.find())
    {
    token[i++] = m.group(0);
       	/*
    	0=phrase
	1=verb
	2=subject_cat
	3=object_cat
	4=tagged_phrase
	5=count
	6=rank
    	*/
    }
    if((token[0].substring(1,token[0].length()-1)).equals(word.trim()))
    {
    	context.write(new Text(token[0]),new Text(token[1]+" "+token[2]+" "+token[3]+" "+token[5]+" "+token[6]));
//   	phrase_unknown=false;
   }
  }
  }

 public static class PBReducerInter
       extends Reducer<Text,Text,Text,Text>{
       	
       	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException{
                       fw.write("<br><b>Phrase Matches</b><div style=\"overflow: scroll;height: 200px;background-color:powderblue;\">");
                       	for(Text val:values)
                       		{
                       			context.write(key,val);
                       			
                       			fw.write(clean(key.toString()+" "+val.toString())+"<br>");
                       	}
                       	fw.write("</div>");
                       
                       
                       }
                      }


public static class PBMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                    	//if(phrase_unknown==false){
      Pattern p = Pattern.compile("<(.*?)>");
      Matcher m = p.matcher(value.toString());
    int i = 0;
    String[] token = new String[7];
    while(m.find())
    {
    token[i++] = m.group(0);
       	/*
    	0=phrase
	1=verb
	2=subject_cat
	3=object_cat
	
	4=count
	5=rank
    	*/
    }
    if(sub_res.isEmpty()==true && ob_res.isEmpty()==true){
    if ((token[0].substring(1,token[0].length()-1)).equals(word.trim())){
    		need_chain=false;
    	context.write(new Text(token[0]), new Text(token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" "+token[5]));
    }	
   }
   else if(sub_res.isEmpty()==true && ob_res.isEmpty()==false){
     if ( ob_res.equals(token[3])==true ){
    		need_chain=false;
    	context.write(new Text(token[0]), new Text(token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" "+token[5]));
    }
                   
   }
   else if(sub_res.isEmpty()==false && ob_res.isEmpty()==true){
     if (sub_res.equals(token[2])==true ){
    		need_chain=false;
    	context.write(new Text(token[0]), new Text(token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" "+token[5]));
    }               
   }
   else if(sub_res.isEmpty()==false && ob_res.isEmpty()==false){
     if (sub_res.equals(token[2])==true && ob_res.equals(token[3])==true ){
    		need_chain=false;
    	context.write(new Text(token[0]), new Text(token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" "+token[5]));
    }
    }               
   //}
  }
 }
  public static class Tuple{
  	String verb="";
  	String sub="";
  	String ob="";
  	int count;
  	int rank;
  	Tuple(String verb,String sub, String ob, String count, String rank){
  		this.verb=verb;
  		this.sub=sub;
  		this.ob=ob;
  		this.count=Integer.parseInt(count.substring(7,count.length()-1));
  		this.rank=Integer.parseInt(rank.substring(6,rank.length()-1));
  	}
  }
  public static class PBMapper1
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                    	
      Pattern p = Pattern.compile("<(.*?)>");
      Matcher m = p.matcher(value.toString());
    int i = 0;
    String[] token = new String[7];
    while(m.find())
    {
    token[i++] = m.group(0);
       	/*
    	0=phrase
	1=verb
	2=subject_cat
	3=object_cat
	4=count
	5=rank
    	*/
    }
    Tuple t = new Tuple(token[1],token[2],token[3],token[4],token[5]);               
   tup.add(t);
   context.write(new Text(token[0]),new Text(token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" "+token[5]));
   
  }
 }
  
  public static class PBReducer1
       extends Reducer<Text,Text,Text,Text>{
       	
       	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException{
        String subject="",object="",verb="";
        int count=0,rank=0;
        int max_count=0,max_rank=Integer.MAX_VALUE;
                       if(need_chain==true){
                       	fw.write("<br><b>Ancestor Matches</b><div style=\"overflow:scroll;height: 200px;background-color:powderblue;\">");
                       if(sub_res.isEmpty()==false && ob_res.isEmpty()==false)
                       {
                       	for(int i=0;i<sub_list.size();i++)
                       		for(int j=0;j<ob_list.size();j++)
                       			{
                       				for(int k=0;k<tup.size();k++)
                       					{
                       						sub_res=sub_list.get(i); ob_res=ob_list.get(j);
                       						//System.out.println("====Comparing\n===="+tup.get(k).sub+"+"+"<"+sub_res+">"+"\n"+tup.get(k).ob+"+"+"<"+ob_res+">");
                       						if((tup.get(k).sub).equals(sub_res) && (tup.get(k).ob).equals(ob_res))
                       						{
                       							if(tup.get(k).rank < max_rank)
                       							if(tup.get(k).count > max_count)
                       								{
                       									max_count = tup.get(k).count;
                       									subject=tup.get(k).sub;
                       									object=tup.get(k).ob;
                       									rank=tup.get(k).rank;
                       									max_rank=tup.get(k).rank;
                       									verb=tup.get(k).verb;
                       								}
                       					}
                       					}
                       			}
                       }
                       else if(sub_res.isEmpty()==true && ob_res.isEmpty()==false)
                       {
                       	for(int j=0;j<ob_list.size();j++)
                       
                       			{
                       				for(int k=0;k<tup.size();k++)
                       					{
                       						ob_res=ob_list.get(j);
                       						//System.out.println("====Comparing\n===="+tup.get(k).ob+"+"+"<"+ob_res+">");
                       						if((tup.get(k).ob).equals(ob_res))
                       						{
                       							if(tup.get(k).rank < max_rank)
                       							if(tup.get(k).count > max_count)
                       								{
                       									max_count = tup.get(k).count;
                       									subject=tup.get(k).sub;
                       									object=tup.get(k).ob;
                       									max_rank=rank=tup.get(k).rank;
                       									verb=tup.get(k).verb;
                       								}
                       					}
                       					}
                       			}
                       }
                       else if(sub_res.isEmpty()==false && ob_res.isEmpty()==true)
                       {
                       	for(int i=0;i<sub_list.size();i++)
                       			{
                       				for(int k=0;k<tup.size();k++)
                       					{
                       						sub_res=sub_list.get(i);
                       						//System.out.println("====Comparing\n===="+tup.get(k).sub+"+"+"<"+sub_res+">");
                       						if((tup.get(k).sub).equals(sub_res))
                       						{
                       							if(tup.get(k).rank < max_rank)
                       							if(tup.get(k).count > max_count)
                       								{
                       									max_count = tup.get(k).count;
                       									subject=tup.get(k).sub;
                       									object=tup.get(k).ob;
                       									max_rank=rank=tup.get(k).rank;
                       									verb=tup.get(k).verb;
                       								}
                       							
                       					}
                       					}
                       			}
                       }
        context.write(new Text("Subject: "+subject+"\n"+"Verb: "+verb+"\n"+"Object: "+object+"\n"+"Count: <count "+count+">\n"+"rank: <rank "+rank+">"), new Text());
        fw.write("Subject: "+clean(subject)+"<br>"+"Verb: "+clean(verb)+"<br>"+"Object: "+clean(object)+"<br>"+"Count: "+max_count+" <br>"+"rank: "+rank+"<br>");
                       }
                       fw.write("</div>");
                    }

       }
  
  
  
  
  public static class PBReducer
       extends Reducer<Text,Text,Text,Text>{
       	
       	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException{
                       if(need_chain==false){
                       int max = 0;
                       int rank = 0;
                       int max_rank=Integer.MAX_VALUE;
                       String result_sub="";
                       String result_ob="";
                       String result_verb="";
                       String result_count="";
		       String result_rank="";
                       fw.write("<br><b>Pattern Matches</b><br><div style=\"overflow:scroll;height: 200px;background-color:powderblue;\">");
                       for(Text val: values){
                       		Pattern p = Pattern.compile("<(.*?)>");
			      Matcher m = p.matcher(val.toString());
			    int i = 0;
			    String[] token = new String[6];
			    while(m.find())
			    {
			    token[i++] = m.group(0);
			       	/*
				0=verb
				1=subject_cat
				2=object_cat
				
				3=count
				4=rank
			    	*/
			    }
			    if(sub_res.isEmpty()==false && ob_res.isEmpty()==false){
			    if(Integer.parseInt(token[4].substring(6,token[4].length()-1)) < max_rank) {	
			    	max = Integer.parseInt(token[3].substring(7,token[3].length()-1));
			    	max_rank = Integer.parseInt(token[4].substring(6,token[4].length()-1));
			    	result_sub = token[1];
			    	result_ob = token[2];
			    	result_verb = token[0];
			    	result_count = token[3];
			    	result_rank = token[4];
			    }
			    	
			    else if(Integer.parseInt(token[4].substring(6,token[4].length()-1)) == max_rank)
                       		   if(Integer.parseInt(token[3].substring(7,token[3].length()-1)) > max){
                       		   	max = Integer.parseInt(token[3].substring(7,token[3].length()-1));
                       		   	max_rank = Integer.parseInt(token[4].substring(6,token[4].length()-1));
                       		   	result_sub = token[1];
                       		   	result_verb = token[0];
				    	result_ob = token[2];
				    	result_count = token[3];
				    	result_rank = token[4];
                       		   }
			    }
			    
			else{
			if(Integer.parseInt(token[4].substring(6,token[4].length()-1)) < max_rank) {	
			    	max = Integer.parseInt(token[3].substring(7,token[3].length()-1));
			    	max_rank = rank = Integer.parseInt(token[4].substring(6,token[4].length()-1));
			    	result_sub = token[1];
			    	result_ob = token[2];
			    	result_verb = token[0];
			    	result_count = token[3];
			    	result_rank = token[4];
			    }
			    	
			    else if(Integer.parseInt(token[4].substring(6,token[4].length()-1)) == max_rank)
                       		   if(Integer.parseInt(token[3].substring(7,token[3].length()-1)) > max){
                       		   	max = Integer.parseInt(token[3].substring(7,token[3].length()-1));
                       		   	max_rank = rank = Integer.parseInt(token[4].substring(6,token[4].length()-1));
                       		   	result_sub = token[1];
                       		   	result_verb = token[0];
				    	result_ob = token[2];
				    	result_count = token[3];
				    	result_rank = token[4];
                       		   }
                       		  }
                     //context.write(key, val); 			
                      } 	
                     // System.out.println("Reducer worked");
                     
                     context.write(new Text("Subject: "+result_sub+"\n"+"Verb: "+result_verb+"\n"+"Object: "+result_ob+"\n"+"Count: "+result_count+"\n"+"rank: "+result_rank), new Text());
                       fw.write("Subject: "+clean(result_sub)+"<br>"+"Verb: "+clean(result_verb)+"<br>"+"Object: "+clean(result_ob)+"<br>"+"Count: "+clean(result_count)+"<br>"+"rank: "+clean(result_rank)+"<br>");
                       //need_chain=false;
                      }
                      else fw.write("No Match Found"+"<br>");
                    fw.write("</div>");
                    }

       }

public static class AttachChainMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

       			Pattern p = Pattern.compile("<(.*?)>");
			   Matcher m = p.matcher(value.toString());
			    //int i = 0;
			    String token = new String();
			    while(m.find())
			    {
			    token = m.group(0);
			   }
			   token = token.replace("<","");
			   token = token.replace(">","");
			   //System.out.println(token);
			   StringTokenizer itr = new StringTokenizer(token);
			   String temp="";
			   if(itr.hasMoreTokens())
			   	temp = itr.nextToken();
			   //System.out.println(temp);
			   if(("<"+temp+">").equals(sub_res)){
			   	System.out.println(sub_res+"+++"+temp);
			   while(itr.hasMoreTokens())
			   	sub_list.add("<"+itr.nextToken()+">");
			   	context.write(new Text("subject chain: "),new Text(token));
			   	    fw.write("<br><div><b>SUBJECT HEIRARCHY</b><br>"+token+"<br>");
			  }
			  if(("<"+temp+">").equals(ob_res)){
			  	System.out.println(ob_res+"+++"+temp);
			   while(itr.hasMoreTokens())
			   	ob_list.add("<"+itr.nextToken()+">");
			   	context.write(new Text("object chain: "),new Text(token));
			   	fw.write("<br><div><b>OBJECT HEIRARCHY</b><br>"+token+"<br>");
			  }	

}
}

public static class AttachChainReducer
       extends Reducer<Text,Text,Text,Text>{
       	
       	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException{
	for(Text val:values)
	context.write(key, val);
}
}
public static void main(String[] args) throws Exception {
	
    //path of your output file (html) in local disk
    String outputFileHtml = "/home/hduser/Desktop/output/";
    
    //EDIT PATHS FOR HDFS HERE
    
    //Save the datasets here
    String DBpediaOntologies = "/new_onto2"; // reads the dataset containing all wikipedia 1.<entities> 2. its <ontology> (class) and 3. its <resource_ID>
    String DBpediaOntology = "/DBOchain.txt"; // reads the dataset that has ontology hierarchy for every class od DBpedia ontology
    String phraseBaseKB = "/pb_w_unique_ranks"; // reads the Knowledge base constructed from wikipedia 
    
    //directories for storing result and intermediate data, hadoop creates/overwrites these in HDFS if used for writing data into it
    String taggerResults = "/pb_tagger_results/"; // for storing the result of tagme on input sentence is stored here
    String intermediateDirectory = "/pb_output_intermediate_stage/"; // for storing intermediate results
    String resultForInput = "/pb_output_final/"; // for storing result for input sentence
    String parentOntologyChain = "/pb_output_attached_chains/"; // for storing ontology hierarchy chain if input pattern didn't match any from KB
    
    
    //set up pipeline
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos, parse");
    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    
    
    
    String entireFileText = new Scanner(new File(args[0])).useDelimiter("\\A").next();
    System.out.println(entireFileText);
    Reader reader = new StringReader(entireFileText);
	DocumentPreprocessor dp = new DocumentPreprocessor(reader);
	List<String> sentenceList = new ArrayList<String>();

	for (List<HasWord> sentence : dp) {
	   // SentenceUtils not Sentence
	   String sentenceString = SentenceUtils.listToString(sentence);
	   sentenceList.add(sentenceString);
	}
	
    String NP="";
    VP="";
    String subject="";
    String object="";
    word="";
    //Configuration[] conf = new Configuration[sentenceList.size()];
    //Job[] job = new Job[sentenceList.size()];
	int i=0, j=0;
	for (String sentence : sentenceList) {
	
	fw = new FileWriter(outputFileHtml+sentence.replace(" ","_")+"html");
	//fw.write("{phraseBaseMap}\n");
	fw.write("<html><head><title>Sentence Evaluation</title></head><body>");
	fw.write("<div><b>SENTENCE</b><br>"+sentence+"<br></div>");
	String s1 = new String ("https://tagme.d4science.org/tagme/tag?lang=en&gcube-token=17bf7977-7ffb-4051-a480-7edfeac6045f-843339462&text=");
        String temp = sentence; 
        String s2 = temp.replace(" ","%20");
        s1=s1.concat(s2); 
        URL oracle = new URL(s1);
        URLConnection yc = oracle.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
        String inputLine;
        inputLine = in.readLine();
        Map annot;
        String[] tag_id;
        String[] tag_title;
        String[] tag_prob;

        JSONObject jsonObject = new JSONObject(inputLine);
	JSONArray friends = jsonObject.getJSONArray("annotations");
	tag_id = new String[friends.length()];
	tag_title = new String[friends.length()];
	tag_prob = new String[friends.length()];
	int index;
	for (index=0; index<friends.length(); ++index){
		JSONObject currentFriend = friends.getJSONObject(index);
		tag_id[index] = currentFriend.getString("id");
		tag_title[index] = currentFriend.getString("spot");
		tag_prob[index] = currentFriend.getString("link_probability");	
        	System.out.println(tag_id[index]+" "+tag_title[index]+" "+tag_prob[index]);
        }
	// create an empty Annotation just with the given text
	Annotation document = new Annotation(sentence);

	// run all Annotators on this text
	pipeline.annotate(document);

	// these are all the sentences in this document
	// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
	List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

	for(CoreMap sent: sentences) {
	  Tree sentenceTree = sent.get(TreeCoreAnnotations.TreeAnnotation.class);
	  
    TregexPattern nounPhraseTregexPattern = TregexPattern.compile("VP");
    TregexMatcher nounPhraseTregexMatcher = nounPhraseTregexPattern.matcher(sentenceTree);
    Double sub_prob=0.0;
    Double ob_prob=0.0;
    while (nounPhraseTregexMatcher.find()) {
    Tree tree = nounPhraseTregexMatcher.getMatch();
    VP = new String(tree.toString());
    
	TregexPattern nounPhraseTregexPattern1 = TregexPattern.compile("NP");
    	TregexMatcher nounPhraseTregexMatcher1 = nounPhraseTregexPattern1.matcher(tree);
        while (nounPhraseTregexMatcher1.find()) {
        NP = nounPhraseTregexMatcher1.getMatch().toString();
        //System.out.println(NP);
        }
    VP = VP.replace(NP,"");
    VP = VP.substring(4,VP.length()-1);
    System.out.println(VP);
    if(VP.isEmpty())
    {
    	continue;
    }
    
    else{
    
    word="";
    int k=0;
    while(k < VP.length())
    {
    	if(VP.charAt(k) == ' ')
    	{
    	k++;
    	int flag =0;
    	if(!(k >= VP.length()))
    		while(VP.charAt(k) != ')' && VP.charAt(k) != '(' )
    		{
    			flag = 1;
    			word += VP.charAt(k++); 
    		}
    		if(flag == 1)
    			word += " ";
    	}
    	k++;
    }
StringTokenizer itr = new StringTokenizer(sentence,word);
subject = sentence.substring(0,sentence.indexOf(word.trim())).trim();
object = (sentence.replace(subject,"")).replace(word,"").trim();
System.out.println(subject+"+++"+object);
sub_tag="";
ob_tag="";
sub_res="";
ob_res="";
double threshold = 0.75;
for(int z=0;z<index;z++){
	if((subject).contains(tag_title[z].trim()) && Double.parseDouble(tag_prob[z]) > threshold)
		{
			sub_tag = tag_id[z];
			sub_prob = Double.parseDouble(tag_prob[z]);
	}
	if((object).contains(tag_title[z].trim()) && Double.parseDouble(tag_prob[z]) > threshold)
		{
			ob_tag = tag_id[z];
			ob_prob = Double.parseDouble(tag_prob[z]);
	}
}	
System.out.println(sub_tag+" "+sub_res+"\n"+ob_tag +" "+ob_res);

}
}

fw.write("<div><b>TAGME TAGGER RESULT</b><br>"+"Subject tag id: "+clean(sub_tag)+"<br>");
fw.write("Probability: "+sub_prob+"<br>");
fw.write("Object tag id: "+clean(ob_tag)+" ");
fw.write("Probability: "+ob_prob+"<br></div>\n");
System.out.println("now running the job");
if(sub_tag.isEmpty()==true)
	flag0=1;
else flag0=0;
if(ob_tag.isEmpty()==true)
	flag1=1;
else flag1=0;

    Configuration conf = new Configuration();
    Configuration conf1 = new Configuration();
    Configuration conf2 = new Configuration();
    Configuration conf4 = new Configuration();
    Configuration conf5 = new Configuration();
    
    //conf[i].set(VP);
    Job job1 = Job.getInstance(conf1,"tagger check");
    Job job = Job.getInstance(conf, "comparing phrase; intermediate stage");
    Job job2 = Job.getInstance(conf2, "pattern found");
    Job job4 = Job.getInstance(conf4, "finding common ancestors");
    Job job5 = Job.getInstance(conf5, "pattern w/ common ancestors");
    
    job1.setJarByClass(PhraseBase.class);
    job1.setMapperClass(TagmeMapper.class);
    job1.setReducerClass(TagmeReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(DBpediaOntologies));
    FileOutputFormat.setOutputPath(job1, new Path(taggerResults+sentence.replace(" ","_")));
    job1.waitForCompletion(true);
    phrase_unknown=true;
    
    job.setJarByClass(PhraseBase.class);
    job.setMapperClass(PBMapperInter.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(PBReducerInter.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
//    FileInputFormat.addInputPath(job, new Path("/pb_final"));
    FileInputFormat.addInputPath(job, new Path(phraseBaseKB));
    FileOutputFormat.setOutputPath(job, new Path(intermediateDirectory+sentence.replace(" ","_")));
    
    job.waitForCompletion(true);
    
    need_chain=true;
     job2.setJarByClass(PhraseBase.class);
    job2.setMapperClass(PBMapper.class);
    job2.setReducerClass(PBReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(intermediateDirectory+sentence.replace(" ","_")));
    FileOutputFormat.setOutputPath(job2, new Path(resultForInput+sentence.replace(" ","_")));
    
     job2.waitForCompletion(true);
     FileSystem hdfs;
     if(need_chain==true)
   	{
   		hdfs = FileSystem.get(conf2);
		hdfs.delete(new Path(resultForInput+sentence.replace(" ","_")), true);

    	sub_list=new ArrayList<String>();
    	ob_list=new ArrayList<String>();
    	//Job job4 = Job.getInstance(conf4, "attach chains to query sub/obj");
    job4.setJarByClass(PhraseBase.class);
    	job4.setMapperClass(AttachChainMapper.class);
    job4.setReducerClass(AttachChainReducer.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path(DBpediaOntology));
    FileOutputFormat.setOutputPath(job4, new Path(parentOntologyChain+sentence.replace(" ","_")));
    job4.waitForCompletion(true);
    int a=0,b=0;
 //   fw.write("<div><b>SUBJECT HEIRARCHY</b><br>");
    for(int p=0;p<sub_list.size();p++)
    	{
    		System.out.println(sub_list.get(p));
    	//	fw.write(sub_list.get(p)+" ");
    }
   // fw.write("<br>");
    fw.write("<b>OBJECT HEIRARCHY</b><br>");
    for(int p=0;p<ob_list.size();p++)
    {
    		System.out.println(ob_list.get(p));
    	//	fw.write(ob_list.get(p)+" ");
    }
    //fw.write("<br><div>");
    tup = new ArrayList<Tuple>();
    job5.setJarByClass(PhraseBase.class);
    job5.setMapperClass(PBMapper1.class);
    job5.setReducerClass(PBReducer1.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job5, new Path(intermediateDirectory+sentence.replace(" ","_")));
    FileOutputFormat.setOutputPath(job5, new Path(resultForInput+sentence.replace(" ","_")));
   job5.waitForCompletion(true);
   
  
   }
    }   
	  i++;
	  j++; 
	 // fw.write("\n{phraseBaseMap}");
	  fw.close();
	}
}
}
