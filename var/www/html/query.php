<?php

include_once 'orion/orion.php';

$result = queryCB();
// var_dump($result);
// exit();
?>

<head>
	<title>Objetos ContextBroker</title>
</head>
<body>
	<table border="1" style="width:90%">
		<?php
		foreach ($result as $row) {
			echo "<tr>";
				// var_dump($row);
				// exit();
			foreach ($row["contextElement"]["attributes"] as $item) {
				// var_dump($item);
				echo "<td>".$item['value']."</td>";
				if($item['name'] == "taxiId"){
					// clearContextBroker($item["value"]);
				}
			}
			echo "</tr>";
		}
		?>
	</table>
</body>
</html>
