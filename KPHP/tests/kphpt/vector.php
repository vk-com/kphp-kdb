@ok benchmark
<?php
  $a = array();
  for ($i = 0; $i < 250000; $i++) {
    $a[] = true;
    $a[] = 5;
    $a[] = "1";
  }
  var_dump (count ($a));
