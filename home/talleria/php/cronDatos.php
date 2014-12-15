<?php
include_once("include.inc.php");

$rows = $db->getObjects("select layerName,city from layers where periodicy = 5 order by city asc");
foreach($rows as $row){
	$layer = $row->layerName;
	$city = $row->city;
	switch($layer){
		case "Map":
		case "Kmean":
		case "Money":
			showMessage('python');
			$comando = "python $pythonDirectory/prev_csv_generator.py $city $layer";
			system($comando);

			$srcFile = "/home/smartaxi/cities/Barcelona/".$layer."/previo.csv";
			$dstFile = "/user/smartaxi/barcelona-".strtolower($layer)."-previous/previo-".time().".csv";
			sftpUpload($srcFile,$dstFile);

		break;
	}
}



function sftpUpload($srcFile,$dstFile){
	$user = ""; 	// Insert User
	$pass = ""; 	// Insert Pass
	$connection = ssh2_connect('130.206.80.46', 2222);
	ssh2_auth_password($connection, $user, $pass);
	$sftp = ssh2_sftp($connection);

	$sftpStream = @fopen('ssh2.sftp://'.$sftp.$dstFile, 'w');

	try {

	    if (!$sftpStream) {
	        throw new Exception("Could not open remote file: $dstFile");
	    }
	    
	    $data_to_send = @file_get_contents($srcFile);
	    
	    if ($data_to_send === false) {
	        throw new Exception("Could not open local file: $srcFile.");
	    }
	    
	    if (@fwrite($sftpStream, $data_to_send) === false) {
	        throw new Exception("Could not send data from file: $srcFile.");
	    }
	    
	    fclose($sftpStream);
	                    
	} catch (Exception $e) {
	    error_log('Exception: ' . $e->getMessage());
	    fclose($sftpStream);
	}
}


?>