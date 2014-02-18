<?php

$filename = $argv[1];

$port = 17239;

$eng = new Memcache;
$eng->connect('localhost', $port, 10000) or die ("Could not connect");

$f = file ($filename);
assert ($f);

$ts = 0;
$td = 0;

$i = 0;
foreach ($f as $s) {
  if ($i == 0) {
    if ($s == "0\n") {
      $prefix = "geoip";
    } else if ($s == "1\n") {    
      $prefix = "cacheip";
    } else if ($s == "2\n") {
      $prefix = "geoipR";
    } else {
      die ("Unknown version $s\n");
    }

    echo $prefix . "\n";
    $e = array ();
    for ($j = 0; $j <= 32; $j++) {
      $q = "" . $j;
      if ($j < 10) { $q = "0" . $j; }
      $t = false;
      while ($t === false) {
        $t = $eng->get ($prefix. ".$q.#");
      }
      if (isset ($t["###error###"])) {
        print "Error: not full result";
        return;
      }
      foreach ($t as $k => $v) {
        $e[$q . "." . $k] = $v;
      }

    }
//    $e = $eng->get (array($prefix . ".*"));
    print "Total " . count ($e) . " old rules and " . (count ($f) - 1) . " new rules\n";
    echo $prefix . "\n";
    sleep (10);
    $i = 1;
    continue;
  }
  $z = explode (" ", $s);
  if (intval ($z[1] < 10)) {
    $key = "0" . intval($z[1]) . "." . dechex ((intval($z[0]) >> (32 - intval($z[1]))));
  } else {
    $key = "" . intval($z[1]) . "." . dechex ((intval($z[0]) >> (32 - intval($z[1]))));
  }
  $value = rtrim ($z[2]);
  if (isset ($e[$key]) && $e[$key] == $value) {
    unset ($e[$key]);
    continue;
  }
  assert ($eng->set ($prefix . "." . $key, $value));
//  echo $prefix.".".$key . " => " . $value;
  $ts ++;
  if (isset ($e[$key])) {
    unset ($e[$key]);
  }
}

foreach ($e as $k => $v) {
  assert ($eng->delete ($prefix. "." . $k));
  $td ++;
}

print "Total $ts sets and $td deletes\n";
