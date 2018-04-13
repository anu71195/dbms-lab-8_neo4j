import time,os,json,operator
start_time=time.time();
from py2neo import authenticate,Graph,Node,Relationship,Path
authenticate('localhost:7474',"neo4j","pass")
graph=Graph();
tx=graph.begin();
graph.run("match(n) detach delete(n)")
print(time.time()-start_time)

##################################################################################
#CONSIDER THE SITUATION THE FIRST TIME PROGRAM IS RUN THEN DROP WILL THROW ERROR
#AND IF DROP IS COMMENTED AND CREATE CONSTRAINT IS RUN AND THE PROGRAM IS RUN FOR
#SECOND TIME THEN IT WILL THROW ERROR OF CONSTRAINT ALREADY OCCURS
##################################################################################

tx.run("drop constraint on (tid_label:tid) ASSERT tid_label.tid is UNIQUE");
tx.run("create constraint on (tid_label:tid) ASSERT tid_label.tid is UNIQUE")
# print(time.time()-start_time)

tx.run("drop constraint on (author_label:author) ASSERT author_label.author_id is UNIQUE")
tx.run("create constraint on (author_label:author) ASSERT author_label.author_id is UNIQUE")
# print(time.time()-start_time)

tx.run("drop constraint on (hashtag_label:hashtag) ASSERT hashtag_label.hashtag is UNIQUE")
tx.run("create constraint on (hashtag_label:hashtag) ASSERT hashtag_label.hashtag is UNIQUE")
# print(time.time()-start_time)

tx.run("drop constraint on (url_label:url) ASSERT url_label.url is UNIQUE")
tx.run("create constraint on (url_label:url) ASSERT url_label.url is UNIQUE")
# print(time.time()-start_time)

tx.run("drop constraint on (keywords_label:tid) ASSERT keywords_label.keyword is UNIQUE")
tx.run("create constraint on (keywords_label:tid) ASSERT keywords_label.keyword is UNIQUE")
# print(time.time()-start_time)

tx.run("drop constraint on (mentions_label:tid) ASSERT mentions_label.mentions is UNIQUE")
tx.run("create constraint on (mentions_label:tid) ASSERT mentions_label.mentions is UNIQUE")

# tx.run("drop constraint on (imagelabel:image)ASSERT imagelabel.image_url is UNIQUE")
# tx.run("create constraint on (imagelabel:image)ASSERT imagelabel.image_url is UNIQUE")
# tx.run("drop constraint on (location_label:location)ASSERT location_label.location is UNIQUE")
# tx.run("create constraint on (location_label:location)ASSERT location_label.location is UNIQUE")
# tx.run("drop constraint on (lang_label:lang)ASSERT lang_label.lang is UNIQUE")
# tx.run("create constraint on (lang_label:lang)ASSERT lang_label.lang is UNIQUE")


tx.commit();
print(time.time()-start_time)

glob_dictionary={}
quoted_dict={}
replyto_source_dict={}
retweet_source_dict={}
current_file_number=0;
for x in sorted(os.listdir('./workshop_dataset1')):
	x='./workshop_dataset1/'+x
	fp=open(x,'r');
	text=json.load(fp);
	fp.close();
	current_file_number+=1;
	print("progress is = ",current_file_number,"/",113,"time=",time.time()-start_time)	
	print(x)
	query="""
	WITH {text} as data
	unwind extract (key in keys(data) | data[key]) as twit
	merge(ltid:tid{tid:twit.tid})ON CREATE
		SET ltid.quote_count=twit.quote_count,ltid.like_count=twit.like_count,ltid.reply_count=twit.reply_count,ltid.verified=twit.verified,ltid.sentiment=twit.sentiments,ltid.retweet_count=twit.retweet_count,ltid.type=twit.type,ltid.tweet_text=twit.tweet_text
	merge(llang:lang{lang:twit.lang})
	merge(ltid)-[:has_lang]-(llang)
	merge(lauthor:author_id{author_id:twit.author_id}) 
	merge(lauthorn:author_name{author_name:twit.author})
	merge(lauthors:authorsn{author_sn:twit.author_screen_name})
	merge(lauthor)-[:has_sname]-(lauthors)
	merge(lauthor)-[:has_name]-(lauthorn)
	merge(ltid)-[:has_author_id]-(lauthor)

	merge(ldate:dt{dtt:twit.datetime}) ON CREATE
		SET ldate.date=twit.date
	FOREACH (hashtagv in twit.hashtags|MERGE(lhashtag:hashtag{hashtag:hashtagv})
	merge (ltid)-[:has_hashtag]-(lhashtag))

	FOREACH (keywordv in twit.keywords_processed_list|MERGE(lkeyword:keyword{keyword:keywordv})
	merge(ltid)-[:has_keyword]-(lkeyword))

	FOREACH (mentionv in twit.mentions|MERGE(lmention:mention{mention:mentionv})
	merge(ltid)-[:has_mention]-(lmention))

	foreach(retweetv in twit.retweet_source_id|merge(lretweet:retweet_id{retweet_id:retweetv})
	merge(ltid)-[:retweet]-(lretweet))

	foreach(sourcev in twit.replyto_source_id|merge(lreply_source:reply_id{reply_id:sourcev})
	merge(ltid)-[:reply_id]-(lreply_source)	)

	foreach(locationv in twit.location|merge(llocation:location{location:locationv})
	merge(ltid)-[:tweeted_from]-(llocation))

	foreach(quotedv in twit.quoted_source_id|merge(lquoted:quoted_source{quoted_source:quotedv})
	merge(ltid)-[:has_quotesi]-(lquoted))	
	"""
	graph.run(query,text=text)
	


print("total time=",time.time()-start_time);