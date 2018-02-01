<?php

include 'config.php';
include 'functions.php';

$db_id = $argv[1];

$con = open_db($db_host,$db_user,$db_pass,$db_name);

if(!sys_lock($con,$db_id)){
	close_db($con);
	exit;
}

gen_database_stat($con,$db_id);

gen_apply_log_backup($con,$db_id);

do_apply_log_backup($con,$db_id);

gen_database_stat($con,$db_id);

sys_lock_release($con,$db_id);
close_db($con);