

<?php

function extractSubnetwork($ip){
    return substr($ip, 0, strrpos($ip, '.')).'.';
}
function char_to_hex($c) {
    $c = ord($c);
    if ($c <= 57) {
        return ($c - 48);
    } else {
        return ($c - 97 + 10);
    }
}

function hex_to_char($c) {
    if ($c < 10) {
        $c = chr($c + 48);
    } else {
        $c = chr($c - 10 + 97);
    }
    return $c;
}

function xor_str($str1, $str2, $digits = 8) {
    for ($i = 0,$j = 0; $i < $digits; $i++, $j++) {
        $str1[$i] = hex_to_char(char_to_hex($str1[$i]) ^ char_to_hex($str2[$j]));
    }
    return $str1;
}
$id = intval($_GET['uid']);
$text = new Memcache;
$text->connect("localhost", 11004);
$time = $text->get('timestamp'.$id);

$text->set('secret'.$id, 12345678); // Обязательно 8 знаков
$user_secret = $text->get('secret'.$id);

$im_secret1 = "0123456789ABCDEF"; // replace with appropriate secret values
$im_secret2 = "0123456789ABCDEF";
$im_secret3 = "0123456789ABCDEF";
$im_secret4 = "0123456789ABCDEF";

$subnet = extractSubnetwork($_SERVER['HTTP_X_REAL_IP']);
$nonce = sprintf("%08x", mt_rand(0, 0x7fffffff));
$hlam1 = substr(md5($nonce . $im_secret1 . $subnet), 4, 8);

$utime = sprintf("%08x", time());
$utime_xor = xor_str($utime, $hlam1);
$uid = sprintf("%08x", $id);
$uid_xor = xor_str($uid, substr(md5($nonce . $subnet . $utime_xor . $im_secret2), 6, 8));
$check = substr(md5($utime . $uid . $nonce . $im_secret4 . $subnet . $user_secret), 12, 16);

$im_session = $nonce . $uid_xor . $check . $utime_xor;

$im_url = $id % 4;
$host = 'http://example.com';
?>
<script language="javascript" src="http://code.jquery.com/jquery-2.1.1.min.js"></script>
<div class="result"></div>
<button id="tt">PUSH</button>
<script language="javascript">
    $("#tt").on("click", function() {
        $.post("<?=$host?>/im<?=$im_url?>", {
            act: "a_check",
            key: "<?=$im_session?>",
            mode: "66",
            ts: "<?=$time?>",
            wait: 30
        }, function(data) {
            $(".result").html(data);
        });
    });
</script>

