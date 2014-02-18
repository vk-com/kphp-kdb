@ok benchmark
<?php
  $a = array (null, 1, false, true, "a");
  $a[] = $a;

  for ($i = 0; $i < 10000; $i++) {
    var_dump ($a);
  }
