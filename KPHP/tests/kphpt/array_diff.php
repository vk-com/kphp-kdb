@ok benchmark
<?php
$i=0; $j=500000;
while($i < 10000) {
	$i++; $j++;
	$data1[] = md5($i);
	$data2[] = md5($j);
}
 
$time = microtime(true);

$data_diff1 = array_diff($data1, $data2);

$time = microtime(true) - $time;

