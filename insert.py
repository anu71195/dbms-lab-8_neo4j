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



def get_query(text):
	dates=[];
	tid=[];
	my_dict={}
	tx=graph.begin()
	count=0;
	for i in text:
		dates.append(text[i]['datetime'])
		tid.append(i)
		my_dict[i]=text[i]['datetime']
	dates, tid = zip(*sorted(zip(dates, tid)))

	for i in range(len(tid)):
		count+=1;
		ttid=tid[i]
		if (count%100==0):
			print(count,time.time()-start_time)
		tid_label=Node("tid",tid=ttid,lang=text[ttid]["lang"],location=text[ttid]["location"],date=text[ttid]["date"],datetime=text[ttid]["datetime"],quote_count=text[ttid]["quote_count"],reply_count=text[ttid]["reply_count"],like_count=text[ttid]["like_count"],verified=text[ttid]["verified"],sentiment=text[ttid]["sentiment"],type=text[ttid]["type"],tweet_text=text[ttid]["tweet_text"])
		author_label=Node("author",author_id=text[ttid]["author_id"])#,author_sname=text[ttid]["author_screen_name"]);
		author_name_label=Node("author_name",author_name=text[ttid]["author"])
		author_sname_label=Node("author_sname",author_sname=text[ttid]["author_screen_name"])
# 		# image_label=Node("image",image_url=text[ttid]["author_profile_image"])
# 		# date_label=Node("date",date=text[ttid]["date"],datetime=text[ttid]["datetime"])
# 		# location_label=Node("location",location=text[ttid]["location"])
# 		# lang_label=Node("lang",lang=text[ttid]["lang"])
# 		# lang_relation=Relationship(tid,"was tweeted in language",lang_label)
# 		# location_tid_relationship=Relationship(tid,'was tweeted from',location_label)
# 		# date_tid_relationship=Relationship(tid,"was tweeted on",date_label)
# 		# author_image_relationship=Relationship(author_label,"has image",image_label)
		author_relationship=Relationship(tid_label,"has_author",author_label)
		author_s_relationship=Relationship(tid_label,"has_s_author",author_sname_label)
		author_name_relationshipo=Relationship(author_label,"has_name",author_name_label)
		glob_dictionary[ttid]=tid_label
		tx.merge(tid_label)
		tx.merge(author_label)
		tx.merge(author_relationship)
		tx.merge(author_name_relationshipo)
		tx.merge(author_s_relationship)
# 		# tx.merge(lang_relation)
# 		# tx.merge(location_tid_relationship)
# 		# tx.merge(date_tid_relationship)
# 		# tx.merge(author_image_relationship)
# 		# print("here")
# 		# try:
# 		# 	tx.merge(author_relationship)
# 		# except:
# 		# 	print(author_label)
# 		# 	exit();
# 		# print("here")

		if(text[ttid]["quoted_source_id"]!=None):
			quoted_dict[ttid]=text[ttid]["quoted_source_id"]
		if(text[ttid]["retweet_source_id"]!=None):
			retweet_source_dict[ttid]=text[ttid]["retweet_source_id"]
		if(text[ttid]["replyto_source_id"]!=None):
			replyto_source_dict[ttid]=text[ttid]["replyto_source_id"]


		if(text[ttid]["hashtags"]!=None):
			for hasht_val in text[ttid]["hashtags"]:
				hashtag_label=Node("hashtag",hashtag=hasht_val);
				hashtag_relationship = Relationship(tid_label, "has_hashtag", hashtag_label)
				tx.merge(hashtag_label)
				tx.merge(hashtag_relationship)

		# if(text[ttid]["url_list"]!=None):
		# 	for url_val in text[ttid]["url_list"]:
		# 		url_label=Node("url",url=url_val)
		# 		url_relationship=Relationship(tid_label,"has url_list",url_label)
		# 		tx.merge(url_label)
		# 		tx.merge(url_relationship)
		if(text[ttid]["keywords_processed_list"]!=None):
			for key_val in text[ttid]["keywords_processed_list"]:
				keywords_label=Node("keywords",keyword=key_val)
				keywords_label_relationship=Relationship(tid_label,"has_keyword",keywords_label)
				tx.merge(keywords_label)
				tx.merge(keywords_label_relationship)
		if(text[ttid]["mentions"]!=None):
			for mention_val in text[ttid]["mentions"]:
				mentions_label=Node("mentions",mentions=mention_val)
				mentions_label_relationship=Relationship(tid_label,"has_mention",mentions_label)
				tx.merge(mentions_label)
				tx.merge(mentions_label_relationship)
	tx.commit();
	

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
	get_query(text)

tx=graph.begin();
count=0;
for i in retweet_source_dict:
	count+=1;
	if(count%100==0):
		print(count,time.time()-start_time)
	if(retweet_source_dict[i] not in glob_dictionary):
		tid_label=Node("tid",tid=retweet_source_dict[i])
	else:
		tid_label=glob_dictionary[retweet_source_dict[i]]
	tx.merge(Relationship(glob_dictionary[i],"retweeted_from",tid_label))
tx.commit()

tx=graph.begin()
count=0;
for i in replyto_source_dict:
	count+=1;
	if(count%100==0):
		print(count,time.time()-start_time)
	if(replyto_source_dict[i] not in glob_dictionary):
		tid_label=Node("tid",tid=replyto_source_id[i])
	else:
		tid_label=glob_dictionary[replyto_source_id[i]]
	tx.merge(Relationship(glob_dictionary[i],"replied_to",tid_label))
tx.commit()

tx=graph.begin()
count=0
for i in quoted_dict:
	count+=1;
	if(count%100==0):
		print(count,time.time()-start_time)
	if(quoted_dict[i] not in glob_dictionary):
		tid_label=Node("tid",tid=quoted_dict[i])
	else:
		tid_label=glob_dictionary[quoted_dict[i]]
	tx.merge(Relationship(glob_dictionary[i],"has_quoted_from",tid_label))
tx.commit();

print("total time=",time.time()-start_time);