<?php

$langs = array ();
$tree = array ();
$vertex_num = 0;
$cases = array ();
$cases_num = 0;
$lang_num = 0;
$max_lang = 0;

function add_lang ($lang_id, $z = -1) {
  global $langs;
  global $lang_num;
  global $max_lang;

  if ($z == -1) {
    $langs[$lang_id] = $lang_id;
  } else {
    $langs[$lang_id] = $z;
  }
  if ($lang_id > $max_lang) {
    $max_lang = $lang_id;
  }
}

function add_extra_langs () {
  //  $a = array (8, 11, 19, 52, 777, 888, 999);
  $a = array (11, 19, 52, 777, 888, 999);
  foreach ($a as $x) {
    add_lang ($x, 0);
  }
}
function new_vertex () {
  global $tree;
  global $vertex_num;
  $tree[$vertex_num ++] = array ("real" => 0, "children" => array ());
  return $vertex_num - 1;
}

function free_tree () {
  global $vertex_num;
  $vertex_num = 0;
  new_vertex ();
  new_vertex ();
}

function add_cases ($rule) {
  global $cases;
  global $cases_num;
  if ($rule == "fixed") {
    return;
  }
  assert (is_array($rule));
  foreach ($rule as $case => $end) {
    if (!isset ($cases[$case])) {
      $cases[$case] = $cases_num;
      $cases[$cases_num ++] = $case;
    }
  }

}

function add_tree ($vertex, $pattern, $male_rule, $female_rule) {
  global $tree;

  add_cases ($male_rule);
  add_cases ($female_rule);

  $have_hyphen = 0;
  $have_bracket = 0;

  $l = strlen ($pattern);
  $current_len = 0;
  $tail_len = -1;
  $have_asterisk = 0;
  for ($i = $l - 1; $i >= 0; $i --) {
    $c = $pattern[$i];
    if ($c == '*') {
      $have_asterisk = 1;
      break;
    }
    if ($c == '-') {
      if ($i == $l - 1) {
        $have_hyphen = 1;
      }
      continue;
    }
    if ($c == ')') {
      assert ($have_bracket == 0 && $current_len == 0);
      $have_bracket = 1;
      continue;
    }
    if ($c == '(') {
      assert ($have_bracket);
      $tail_len = $current_len;
      continue;
    }

    if (!isset ($tree[$vertex]["children"][$c])) {
      $tree[$vertex]["children"][$c] = new_vertex ();
    }
    $vertex = $tree[$vertex]["children"][$c];
    $current_len ++;
  }
  if ($have_asterisk == 0) {
    $c = chr (0);
    if (!isset ($tree[$vertex]["children"][$c])) {
      $tree[$vertex]["children"][$c] = new_vertex ();
    }
    $vertex = $tree[$vertex]["children"][$c];
  }
  if ($tree[$vertex]["real"] == 1) {
    print "//!!! dupicate rule for pattern $pattern (old pattern " . $tree[$vertex]["pattern"] . ")\n";
  }
  $tree[$vertex]["real"] = 1;
  $tree[$vertex]["hyphen"] = $have_hyphen;
  $tree[$vertex]["tail_len"] = ($tail_len != -1 ? $tail_len : $current_len);
  $tree[$vertex]["pattern"] = $pattern;
  $tree[$vertex]["male_rule"] = $male_rule;
  $tree[$vertex]["female_rule"] = $female_rule;
  $tree[$vertex]["have_asterisk"] = $have_asterisk;
}

function print_children_array ($lang_id) {
  global $tree;
  global $vertex_num;
  print "const int lang_${lang_id}_children[" . (2 * ($vertex_num - 2)) . "] = {";
  $x = 0;
  for ($i = 0; $i < $vertex_num; $i ++) {
    $tree[$i]["start_children"] = $x;
    for ($_c = 0; $_c < 256; $_c ++) {
      $c = chr ($_c);
      if (isset ($tree[$i]["children"][$c])) {
        if ($x != 0) {
          print ",";
        }
        print $_c . "," . $tree[$i]["children"][$c];
        $x ++;
      }
    }
    $tree[$i]["end_children"] = $x;
  }
  assert ($x == $vertex_num - 2);
  print "};\n";
}

function print_endings ($rule) {
  assert (is_array ($rule));
  global $cases;
  global $cases_num;
  $y = 0;
  for ($i = 0; $i < $cases_num; $i++) {
   if ($y != 0) {
      print ",";
    }
    if (isset ($rule[$cases[$i]])){
      print "\"" . $rule[$cases[$i]] . "\"";
    } else {
      print "0";
    }
    $y ++;
  }
}

function print_endings_array ($lang_id) {
  global $cases;
  global $cases_num;
  global $tree;
  global $vertex_num;
  print "const char *lang_${lang_id}_endings[] = {";
  $x = 0;
  for ($i = 0; $i < $vertex_num; $i ++) if ($tree[$i]["real"] == 1) {
    if ($tree[$i]["male_rule"] == "fixed") {
      $tree[$i]["male_rule_num"] = -1;
    } else {
      if ($x != 0) {
        print ",";
      }
      $tree[$i]["male_rule_num"] = $x ++;
      print_endings ($tree[$i]["male_rule"]);
    }
    if ($tree[$i]["female_rule"] == "fixed") {
      $tree[$i]["female_rule_num"] = -1;
    } else {
      if ($x != 0) {
        print ",";
      }
      $tree[$i]["female_rule_num"] = $x ++;
      print_endings ($tree[$i]["female_rule"]);
    }
  }
  print "};\n";
}

function print_nodes_array () {
  global $tree;
  global $vertex_num;
  for ($i = 0; $i < $vertex_num; $i ++) {
    $v = $tree[$i];
    print "  {";
    if ($v["real"] == 1) {
      print ".tail_len = " . $v["tail_len"] . ",";
      print ".hyphen = " . $v["hyphen"] . ",";
      print ".male_endings = " . $v["male_rule_num"] . ",";
      print ".female_endings = " . $v["female_rule_num"] . ",";
    } else {
      print ".tail_len = -1,";
    }
    print ".children_start = " . $v["start_children"] . ",";
    print ".children_end = " . $v["end_children"];
    if ($i != $vertex_num - 1) {
      print "},\n";
    } else {
      print "}\n";
    }
  }
}
function create_lang ($lang_id, $res) {
  add_lang ($lang_id);
  free_tree ();  
  
  $w_n = 0;
  if (isset ($res["names"])) {
    $w_n = 1;
    foreach ($res["names"] as $rule) {
      foreach ($rule["patterns"] as $pattern) {
        add_tree (0, $pattern, $rule["male"], $rule["female"]);
      }
    }
  }
  $w_s = 0;
  if (isset ($res["surnames"])) {
    $w_s = 1;
    foreach ($res["surnames"] as $rule) {
      foreach ($rule["patterns"] as $pattern) {
        add_tree (1, $pattern, $rule["male"], $rule["female"]);
      }
    }
  }
  print_children_array ($lang_id);
  print_endings_array ($lang_id);
  
  print "struct lang lang_$lang_id = {\n";
  print "  .flexible_symbols = \"" . $res["flexible_symbols"] . "\",\n";
  if ($w_n == 1) {
    print "  .names_start = 0,\n";
  } else {
    print "  .names_start = -1,\n";
  }
  if ($w_s == 1) {
    print "  .surnames_start = 1,\n";
  } else {
    print "  .surnames_start = -1,\n";
  }
  global $cases_num;
  print "  .cases_num = ${cases_num},\n";
  print "  .children = lang_${lang_id}_children,\n";
  print "  .endings = lang_${lang_id}_endings,\n";
  print "  .nodes = {\n";
  print_nodes_array ();
  print "  }\n";
  print "};\n";

}

function print_cases () {
  global $cases;
  global $cases_num;
  print "#define CASES_NUM $cases_num\n";
  print "const char *cases_names[CASES_NUM] = {";
  for ($i = 0; $i < $cases_num; $i++) {
    if ($i != 0) {
      print ",";
    }
    print "\"" . $cases[$i] . "\"";
  }
  print "};\n";
}


function print_langs () {
  global $langs;
  global $max_lang;
  global $lang_num;
  $total_langs = $max_lang + 1;
  print "#define LANG_NUM $total_langs\n";
  print "struct lang *langs[LANG_NUM] = {";
  for ($i = 0; $i < $total_langs; $i++) {
    if ($i > 0) {
      print ",";
    }
    if (isset ($langs[$i])) {
      print "&lang_" . $langs[$i];
    } else {
      print "0";
    }
  }
  print "};\n";
}
print "#include \"vkext_flex.h\"\n";

require ("/var/www/cs7777/data/www/lib/flex.php");
$res = array ();
$res = setupRusFlex (0, $res);

create_lang (0, $res);

require ("/var/www/cs7777/data/www/lib/nonrus_flex.php");
for ($lang_id = 1; $lang_id < 256; $lang_id ++) {
  $res = array ();
  $res = setupNonRusFlex ($lang_id, $res);
  if (!isset ($res["names"])) {
    continue;
  }
  create_lang ($lang_id, $res);
}

print_cases ();
add_extra_langs ();
print_langs ();
?>
