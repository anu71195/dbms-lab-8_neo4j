from py2neo import authenticate,Graph,Node,Relationship
authenticate('localhost:7474',"neo4j","pass")
graph=Graph();
graph.run("match(n) detach delete(n)")
alice = Node("Person", name="Alice")
bob = Node("Person", name="Bob")
alice_knows_bob = Relationship(alice, "KNOWS", bob)
graph.create(alice_knows_bob)
graph.run("CREATE (c:Person {name:{N}}) RETURN c", {"N": "Carol"});
a=graph.run("match(p:Person) return p")
for i in a:
	print(i)