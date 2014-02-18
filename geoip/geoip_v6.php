<?php

$filename = $argv[1];
$port = 17239;

$eng = new Memcache;
$eng->connect('localhost', $port, 10000) or die ("Could not connect");

$f = file ($filename);
assert ($f);

$e = array ();
for ($i = 0; $i <= 99; $i++) {
  $s = "" . $i;
  if ($i < 10) { $s = "0" . $i; }
  $t = false;
  while ($t === false) {
    $t = $eng->get ("geoip_v6.$s.#");
  }
  if (isset ($t["###error###"])) {
    print "Error: not full result";
    return;
  }
  foreach ($t as $k => $v) {
    $e[$s . "." . $k] = $v;
  }

}

//$e = $eng->get (array("geoip_v6.*"));
print "Total " . count ($e) . " old rules and " . count ($f) . " new rules\n";

$ts = 0;
$td = 0;

foreach ($f as $s) {
  $z = explode (" ", $s);
  if (intval ($z[1] < 10)) {
    $key = "0" . intval($z[1]) . ".";
  } else {
    $key = "" . intval($z[1]) . ".";
  }
  $key = $key . substr ($z[0], 0, intval ($z[1]) / 4);
  if ($z[1] % 4 != 0) {
    $c = $z[0][$z[1] / 4];
    $key = $key . sprintf ('%x', (hexdec ($c) >> (4 - ($z[1] % 4))));
  }
  $key = rtrim ($key, "0");
  $value = rtrim ($z[2]);
  if (isset ($e[$key]) && $e[$key] == $value) {
    unset ($e[$key]);
    continue;
  }
  assert ($eng->set ("geoip_v6." . $key, $value));
  //echo $key . " " . $value . "\n";
  $ts ++;
  if (isset ($e[$key])) {
    unset ($e[$key]);
  }
}

foreach ($e as $k => $v) {
  assert ($eng->delete ("geoip_v6." . $k));
  $td ++;
}

print "Total $ts sets and $td deletes\n";
