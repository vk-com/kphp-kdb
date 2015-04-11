<?

    $chunk = 16;
    $chunk_enable = false;
    $one_pix_transparent_png = array(
      137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82,
      0, 0, 0, 1, 0, 0, 0, 1, 8, 4, 0, 0, 0, 181, 28, 12,
      2, 0, 0, 0, 2, 98, 75, 71, 68, 0, 0, 170, 141, 35, 50, 0,
      0, 0, 11, 73, 68, 65, 84, 8, 215, 99, 96, 96, 0, 0, 0, 3,
      0, 1, 32, 213, 148, 199, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66,
      96, 130
    );
    $byte_array = array();
    $h_file_name = "storage-error-png.h";
    $show_byte_array = false;

    $usage = "Usage: {$argv[0]} [options]
    -h\t\tprint this help and exit
    -d\t\tcreate default file (one pixel transparent png)
    -s\t\tshow create byte array
    -f<path>\tpath to png file
    -c<num>\tsplit content by <num> byte (default: {$chunk})\n";

    $opt = getopt("f:c::hsd");
    if(isset($opt['h']) || !count($opt)){
      die($usage);
    }

    if(isset($opt['c'])){
      $chunk_enable = true;
      if(!empty($opt['c']) && is_numeric($opt['c']) && $user_chunk = intval($opt['c'])){
        $chunk = $user_chunk > 0 ? $user_chunk : $chunk;
      }
    }

    if(isset($opt['f']) && !empty($opt['f']) && !isset($opt['d'])){
      $file_name = $opt['f'];
      if(!file_exists($file_name)){
        printf("Error: File '%s' does not exist.\n", $file_name); die();
      }
      if(!is_readable($file_name) || !$bin_data = file_get_contents($file_name)){
        printf("Error: File '%s' does not readable.\n", $file_name); die();
      }
      $byte_array = unpack("C*", $bin_data); //  $byte_array = array_map('hexdec', str_split(bin2hex($bin_data), 2));
      if(array(137, 80, 78, 71, 13, 10, 26, 10) != array_slice($byte_array, 0, 8)){
        printf("Error: File '%s' not a png file.\n", $file_name); die();
      }
    }

    if(isset($opt['d'])){
      $byte_array = $one_pix_transparent_png;
    }

    if(!isset($opt['d']) && !isset($opt['f'])){
      die($usage);
    }

    if(isset($opt['s'])){
      $show_byte_array = true;
    }

    $byte_chunk = array_chunk($byte_array, $chunk);
    $byte_string = !$chunk_enable ? implode(',', $byte_array) : "\n  " . implode(",\n  ", array_map('implode', array_fill(0, count($byte_chunk), ', '), $byte_chunk)) . "\n";
    $content = sprintf(
      "// Image: %s\nstatic const unsigned char one_pix_transparent_png[%d] = {%s};\n",
      $file_name,
      count($byte_array),
      $byte_string
    );

    if(file_put_contents("./{$h_file_name}", $content)){
      if($show_byte_array) echo $content;
      printf("OK: An array of bytes successfully written to the file '%s'\n", $h_file_name);
    } else {
      printf("During recording, an error occurred.\n");
    }
