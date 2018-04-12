import time,os,json,operator
start_time=time.time();
from py2neo import authenticate,Graph,Node,Relationship
authenticate('localhost:7474',"neo4j","pass")
graph=Graph();
graph.run("match(n) detach delete(n)")
##################################################################################
#CONSIDER THE SITUATION THE FIRST TIME PROGRAM IS RUN THEN DROP WILL THROW ERROR
#AND IF DROP IS COMMENTED AND CREATE CONSTRAINT IS RUN AND THE PROGRAM IS RUN FOR
#SECOND TIME THEN IT WILL THROW ERROR OF CONSTRAINT ALREADY OCCURS
##################################################################################

graph.run("drop constraint on (tid_label:tid) ASSERT tid_label.tid is UNIQUE");
graph.run("create constraint on (tid_label:tid) ASSERT tid_label.tid is UNIQUE")


def get_query(text):
	dates=[];
	tid=[];
	my_dict={}
	for i in text:
		dates.append(text[i]['datetime'])
		tid.append(i)
		my_dict[i]=text[i]['datetime']
	dates, tid = zip(*sorted(zip(dates, tid)))
	# for i in range(len(tid)):
		# print(text[tid[i]])
	#graph.run("CREATE(tid_label:tid{tid:{tid_value},quote_count:{quote_count_value},reply_count:{reply_count_value},date_time:{datetime_value},date_:{date_value},like_count:{like_count_value},verified:{verified_value},sentiment:{sentiment_value},location:{location_value},retweet_count:{retweet_value},type:{type_value},tweet_text:{tweet_text_value},lang:{lang_value}})",{"tid_value":"934933919157202944","quote_count_value":"4","reply_count_value":"3","datetime_value":"2017-11-26 23:57:34","date_value":"2017-11-26","like_count_value":"2","verified_value":"False","sentiment_value":"-1","location_value":"United States","retweet_value":"5","type_value":"retweet","tweet_text_value":"...anticipate there will be more lies when Jared turns over the missing records https://t.co/YIDgNC35gB","lang_value":"en"})
	# graph.run("CREATE(hashtag_label:hashtag{hashtag:{hashtag_value}})",{"hashtag_value":"sexy"})
#	graph.run("CREATE(url_label:url{url:{url_value}})",{"url_value":"https://t.co/BXot4XlmjN"})
#	graph.run("CREATE(author_label:author{author_id:{author_id_value},author_image:{author_image_value},author_name:{author_name_value},author_sname:{author_sname_value}})",{"author_id_value":"3045400420","author_image_value":"https://pbs.twimg.com/profile_images/933583536757657600/uWc-OYde_normal.jpg","author_name_value":"Richa Sharma","author_sname_value":"Richasharma0971"})
	# graph.run("CREATE(keywords_label:keywords{keywords:{keywords_value}})",{"keywords_value":"free bets,£40 free today"});
	# graph.run("CREATE(mentions_label:mentions{mentions:{mentions_value}})",{"mentions_value":"OfficeOfRG"});
	#graph.run("CREATE()",{})
	# media_list
	
	tid_label=Node("tid",tid="934933919157202944",quote_count="4",reply_count="3",datetime="2017-11-26 23:57:34",date="2017-11-26",like_count="2",verified="False",sentiment="-1",location="United States",retweet="5",type="retweet",tweet_text="...anticipate there will be more lies when Jared turns over the missing records https://t.co/YIDgNC35gB",lang="en")
	hashtag_label=Node("hashtag",hashtag="sexy");
	url_label=Node("url",url="https://t.co/BXot4XlmjN")
	author_label=Node("author",author_id="3045400420",author_image="https://pbs.twimg.com/profile_images/933583536757657600/uWc-OYde_normal.jpg",author_name="Richa Sharma",author_sname="Richasharma0971");
	keywords_label=Node("keywords",keyword="free bets,£40 free today")
	mentions_label=Node("mentions",mentions="OfficeOfRG")
	media_list=Node("media",media_type="photo",display_url="pic.twitter.com/29kUOncT9F",media_id="934934104037928960",media_url="https://pbs.twimg.com/media/DPmNnTeUMAA0QAB.jpg")
		
	hashtag_relationship = Relationship(tid_label, "has hashtag", hashtag_label)
	url_relationship=Relationship(tid_label,"has url_list",url_label)
	author_relationship=Relationship(tid_label,"has author",author_label)
	keywords_label_relationship=Relationship(tid_label,"has keyword",keywords_label)
	mentions_label_relationship=Relationship(tid_label,"has mention",mentions_label)
	# media_list_relationship=Relationship(tid_label,"has media",media_list)

	graph.create(hashtag_relationship)
	graph.create(url_relationship)
	graph.create(author_relationship)
	graph.create(keywords_label_relationship)
	graph.create(mentions_label_relationship)	
###################################################################################
#put unique and primary constraint everywhere because repeition of nodes will create repeated nodes
##################################################################################

	# replyto_source_id
	# retweet_source_id
	# exit()
	# for i in range(len(tid)):
	# 	if not (my_dict[tid[i]]==dates[i]):
	# 		print("here")




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


	# alice = Node("Person", name="Alice")
	# print(alice)


	exit()


# alice = Node("Person", name="Alice")
# bob = Node("Person", name="Bob")
# alice_knows_bob = Relationship(alice, "KNOWS", bob)
# graph.create(alice_knows_bob)
# graph.run("CREATE (c:Person {name:{N}}) RETURN c", {"N": "Carol"});
# a=graph.run("match(p:Person) return p")
print("total time=",time.time()-start_time);