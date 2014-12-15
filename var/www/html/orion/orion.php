<?php

function raw2CB($rawData) {

	ini_set('memory_limit', '-1'); 

	for ($i=0; $i < count($rawData); $i++) { 

		$row = explode(",", $rawData[$i]);

		if(count($row) != 5) continue;

		$post2array["contextElements"][$i] = array();
		$post2array["contextElements"][$i]["type"] = "Taxi";
		$post2array["contextElements"][$i]["isPattern"] = "false";
		$post2array["contextElements"][$i]["id"] = $row[0];
		$post2array["contextElements"][$i]["attributes"] = array();
		$post2array["contextElements"][$i]["attributes"][] = array('name' => "taxiId", 'type' => 'int', 'value' => $row[0]);
		$post2array["contextElements"][$i]["attributes"][] = array('name' => "time", 'type' => 'timestamp', 'value' => $row[1]);
		$post2array["contextElements"][$i]["attributes"][] = array('name' => "lat", 'type' => 'coord', 'value' => $row[2]);
		$post2array["contextElements"][$i]["attributes"][] = array('name' => "lon", 'type' => 'coord', 'value' => $row[3]);
		$post2array["contextElements"][$i]["attributes"][] = array('name' => "status", 'type' => 'int', 'value' => $row[4]);
	}

	$post2array["updateAction"] = "APPEND";
	$res = uploadCB(json_encode($post2array));
	error_log("To CB: " . count($post2array["contextElements"]));
}



function uploadCB($jsonData) {
	$USER = "";
	$PASSWORD = "";

	$url1 = "http://cloud.lab.fi-ware.org:4730/v2.0/tokens";
	$url2 = "http://orion.lab.fi-ware.org:1026/";


	$post1 = "{\"auth\": {\"passwordCredentials\": {\"username\":\"$USER\", \"password\":\"$PASSWORD\"}}}";


	$handler = curl_init(); 
	curl_setopt($handler, CURLOPT_URL, $url1);  
	curl_setopt($handler, CURLOPT_POST,true);  
	curl_setopt($handler, CURLOPT_POSTFIELDS, $post1);
	curl_setopt($handler, CURLOPT_RETURNTRANSFER, true); 
	$response = curl_exec ($handler);  
	$response = json_decode($response, true);

	$TOKEN = $response["access"]["token"]["id"];

	$header = array();
	$header[] = "x-auth-token: $TOKEN";
	$header[] = "Content-Type: application/json";
	$header[] = "Accept: application/json";

	curl_setopt($handler, CURLOPT_URL, $url2 . "NGSI10/updateContext");
	curl_setopt($handler, CURLOPT_POST,true);  
	curl_setopt($handler, CURLOPT_HTTPHEADER, $header);
	curl_setopt($handler, CURLOPT_RETURNTRANSFER, true);
	curl_setopt($handler, CURLOPT_POSTFIELDS, $jsonData);
	$response = curl_exec ($handler);  
	curl_close($handler);
	
	return $response;
}


function queryCB() {
	$USER = "";
	$PASSWORD = "";

	$url1 = "http://cloud.lab.fi-ware.org:4730/v2.0/tokens";
	$url2 = "http://orion.lab.fi-ware.org:1026/";

	$post1 = "{\"auth\": {\"passwordCredentials\": {\"username\":\"$USER\", \"password\":\"$PASSWORD\"}}}";
	
	$handler = curl_init(); 
	curl_setopt($handler, CURLOPT_URL, $url1);  
	curl_setopt($handler, CURLOPT_POST,true);  
	curl_setopt($handler, CURLOPT_POSTFIELDS, $post1);
	curl_setopt($handler, CURLOPT_RETURNTRANSFER, true); 
	$response = curl_exec ($handler);  
	// curl_close($handler);  

	$response = json_decode($response, true);

	$TOKEN = $response["access"]["token"]["id"];

	$header = array();
	$header[] = "x-auth-token: $TOKEN";
	$header[] = "Content-Type: application/json";
	$header[] = "Accept: application/json";


	$post2array = array();
	$post2array["entities"] = array();
	$post2array["entities"][0] = array();
	$post2array["entities"][0]["type"] = "Taxi";
	$post2array["entities"][0]["isPattern"] = "true";
	$post2array["entities"][0]["id"] = ".*";

	$jsonData = json_encode($post2array);
	$header[] = "Content-Length: " . strlen($jsonData);

	curl_setopt($handler, CURLOPT_URL, $url2 . "NGSI10/queryContext?limit=1000&details=on");  
	curl_setopt($handler, CURLOPT_HTTPHEADER, $header);
	curl_setopt($handler, CURLOPT_RETURNTRANSFER, true);
	curl_setopt($handler, CURLOPT_POSTFIELDS, $jsonData);
	$response = curl_exec ($handler);  
	curl_close($handler);

	$response = json_decode($response, true);

	echo "Ha devuelto ".count($response["contextResponses"])." objetos \n";

	return $response["contextResponses"];
}



function clearContextBroker($id){

	$url2 = "http://orion.lab.fi-ware.org:1026/";

	$post2array = array();
	$post2array["contextElements"] = array();
	$post2array["contextElements"][0] = array();
	$post2array["contextElements"][0]["type"] = "Taxi";
	$post2array["contextElements"][0]["isPattern"] = "false";
	$post2array["contextElements"][0]["id"] = $id;
	$post2array["updateAction"] = "DELETE";

	$jsonData = json_encode($post2array);
	echo "JSON: $jsonData\n";

	// GET TOKEN
	$TOKEN = getToken();

	$header = array();
	$header[] = "x-auth-token: $TOKEN";
	$header[] = "Content-Type: application/json";
	$header[] = "Accept: application/json";
	$header[] = "Content-Length: " . strlen($jsonData);
	

	// CURL
	$handler = curl_init();
	curl_setopt($handler, CURLOPT_URL, $url2 . "NGSI10/updateContext");  
	curl_setopt($handler, CURLOPT_HTTPHEADER, $header);
	curl_setopt($handler, CURLOPT_RETURNTRANSFER, true);
	curl_setopt($handler, CURLOPT_POSTFIELDS, $jsonData);

	$response = curl_exec ($handler);  
	
	curl_close($handler);

	$response = json_decode($response, true);
	var_dump($response);
}


function getToken(){
	$USER = "";
	$PASSWORD = "";

	$url1 = "http://cloud.lab.fi-ware.org:4730/v2.0/tokens";
	$post1 = "{\"auth\": {\"passwordCredentials\": {\"username\":\"$USER\", \"password\":\"$PASSWORD\"}}}";

	
	$handler = curl_init(); 
	curl_setopt($handler, CURLOPT_URL, $url1);  
	curl_setopt($handler, CURLOPT_POST,true);  
	curl_setopt($handler, CURLOPT_POSTFIELDS, $post1);
	curl_setopt($handler, CURLOPT_RETURNTRANSFER, true); 
	$response = curl_exec ($handler);  

	curl_close($handler);  
	// var_dump(json_decode($response));

	$response = json_decode($response, true);

	$TOKEN = $response["access"]["token"]["id"];
	echo $TOKEN;

	//$TOKEN = "UU3NzJz1_fuvfkto5iTdbeNtCBwKg8QSNfSaz29uXFh6yEn2gadGJ9a_RGrOslWEnQTHcL7hX0zucjc0S9SUCw";
	
	return $TOKEN;
}

?>
