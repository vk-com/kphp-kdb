@ok
<?
  for ($i = -300; $i <= 300; $i++) {
    if ($i) {
      echo chr ($i);
      echo ord (chr ($i));
    }
  }
  var_dump (chr(0) == '');
?>
