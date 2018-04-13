<?php
	error_reporting(E_ALL);
	ini_set('display_errors', '1');
require_once 'vendor/autoload.php';

use GraphAware\Neo4j\Client\ClientBuilder;

$client = ClientBuilder::create()
    ->addConnection('default', 'http://neo4j:pass@localhost:7474') // Example for HTTP connection configuration (port is optional)
    ->build();
?>

<!DOCTYPE html>
<html>
<head>
	<title>CS345 Cassandra</title>
</head>
<body>
<form method="post" action="index2.php">
	<input type="text" name="query">
	<input type="submit" name="submit">
	<br>
	<br>


	<?php

		if(!empty($_POST))
		{
			echo "query given is=";
			echo $_POST["query"]; 
			echo "<br>";
			$result = $client->run($_POST["query"]);
			foreach ($result->getRecords() as $record)
			{
				foreach($record->values() as $rv)
				{
					foreach($rv->values() as $rvv)
					{
						echo $rvv."<br>";
					}
				}
				echo "<hr>";
			}



		}
		else{
			echo "Enter a query";
		}
	?>
</form>
</body>
</html>
