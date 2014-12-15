<?php

$fileName = "test_csv.csv";
$srcFile = "/var/www/$fileName";
$dstFile = "/user/smartaxi/$fileName";

$user = "";     // Insert User
$pass = "";     // Insert Pass
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

?>