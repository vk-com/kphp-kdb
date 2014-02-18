@ok benchmark
<?php

  $a = true;
  var_dump ($a);
  $a = 2;
  var_dump ($a);
  $a = 3.0;
  var_dump ($a);
  $a = "4";
  var_dump ($a);
  $a = array (5, "7");
  var_dump ($a);

  for ($i = 0; $i < 10000000; $i++) {
    $a = array (5, null);
  }

  var_dump ($a);

