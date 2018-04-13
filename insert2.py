import time,os,json,operator
start_time=time.time();
from py2neo import authenticate,Graph,Node,Relationship
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
	for i in text:
		print(text[i])
print("total time=",time.time()-start_time);