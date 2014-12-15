<?php

include_once 'orion/orion.php';

$subscriptionIds = array("542dc9e21860a3873d395e0d", "542d12a01860a3873d395dd7", "542c73d81860a3873d395dd2");

$subscriptionId = $subscriptionIds[2];


$USER = "";		// Inser User
$PASSWORD = "";	// Insert Password

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


$jsonData = "{ \"subscriptionId\": \"$subscriptionId\" }";


$header[] = "Content-Length: " . strlen($jsonData);
curl_setopt($handler, CURLOPT_URL, $url2 . "NGSI10/unsubscribeContext");  
curl_setopt($handler, CURLOPT_POST,true);  
curl_setopt($handler, CURLOPT_HTTPHEADER, $header);
curl_setopt($handler, CURLOPT_RETURNTRANSFER, true);
curl_setopt($handler, CURLOPT_POSTFIELDS, $jsonData);
$response = curl_exec ($handler);  
curl_close($handler);



$response = json_decode($response, true);

?>